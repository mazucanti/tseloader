#!/usr/bin/env python3
"""
Low-memory TSE + IBGE loader for a PostgreSQL/PostGIS map database.

Memory model (target: << 1 GB RSS):
  * downloads are streamed to disk in 1 MiB chunks (never held in RAM)
  * CSVs are read row-by-row straight out of the zip (no extraction, no pandas)
  * rows are streamed into UNLOGGED staging tables with COPY
  * aggregation (section -> zone) is done by PostgreSQL, which spills to disk
  * IBGE geometries are fetched one state at a time

Usage:
  python loader.py all                       # geo + votes + zone points
  python loader.py geo
  python loader.py votes [--scope geral municipal] [--uf SP RJ] [--years 2026,2022] [--force]
  python loader.py zones
"""
import argparse, csv, io, json, logging, os, re, resource, sys, tempfile, time, unicodedata, zipfile

import psycopg
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger("loader")

TSE_BASE = "https://cdn.tse.jus.br/estatistica/sead/odsele"
IBGE_LOC = "https://servicodados.ibge.gov.br/api/v1/localidades"
IBGE_MESH = "https://servicodados.ibge.gov.br/api/v3/malhas"

UFS = "AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO".split()
UF_CODES = {"RO": 11, "AC": 12, "AM": 13, "RR": 14, "PA": 15, "AP": 16, "TO": 17, "MA": 21, "PI": 22,
            "CE": 23, "RN": 24, "PB": 25, "PE": 26, "AL": 27, "SE": 28, "BA": 29, "MG": 31, "ES": 32,
            "RJ": 33, "SP": 35, "PR": 41, "SC": 42, "RS": 43, "MS": 50, "MT": 51, "GO": 52, "DF": 53}
# General elections (President/Governor/Senate/Deputies) and municipal elections (Mayor/Councillors)
# are separate families; each is resolved independently, newest year first (2026 -> fallbacks).
DEFAULT_YEARS = {
    "geral": os.getenv("TSE_YEARS_GERAL", "2026,2022,2018,2014"),
    "municipal": os.getenv("TSE_YEARS_MUNICIPAL", "2024,2020,2016,2012"),
}
STAGE_COLS = "ano,turno,cd_cargo,uf,cd_municipio,zona,nr_votavel,nm_votavel,tipo,qt_votos"


# ----------------------------------------------------------------------------- helpers
def norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().upper()
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9]+", " ", s)).strip()


def rss_mb() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # Linux: KiB


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0 (compatible; tse-map-loader/1.0)"
    retry = Retry(total=5, backoff_factor=2, status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=("GET", "HEAD"))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def probe(s, url):
    """HEAD the file. Returns metadata dict if it exists, else None."""
    try:
        r = s.head(url, timeout=(10, 30), allow_redirects=True)
    except requests.RequestException as e:
        log.debug("probe failed %s: %s", url, e)
        return None
    size = int(r.headers.get("Content-Length") or 0)
    if r.status_code == 200 and (size == 0 or size > 1024):
        return {"etag": r.headers.get("ETag"), "last_modified": r.headers.get("Last-Modified"), "size": size}
    return None


def download(s, url, dest):
    with s.get(url, stream=True, timeout=(10, 120)) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)


def sniff_encoding(head: bytes) -> str:
    if any(b > 127 for b in head):
        try:
            head[:-4].decode("utf-8")
            return "utf-8"
        except UnicodeDecodeError:
            pass
    return "latin-1"          # TSE legacy default


def iter_zip_csv(zip_path, wanted, required):
    """Stream selected columns (by header name) from every CSV in the zip, row by row."""
    with zipfile.ZipFile(zip_path) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not names:
            raise RuntimeError(f"no CSV in {zip_path}")
        for name in names:
            with zf.open(name) as raw:
                enc = sniff_encoding(raw.read(1 << 16))
            with zf.open(name) as raw:
                txt = io.TextIOWrapper(raw, encoding=enc, errors="replace", newline="")
                rd = csv.reader(txt, delimiter=";")
                header = [h.strip().lstrip("\ufeff").upper() for h in next(rd)]
                missing = [c for c in required if c not in header]
                if missing:
                    raise RuntimeError(f"{name}: missing columns {missing}")
                pos = [header.index(c) if c in header else None for c in wanted]
                for row in rd:
                    yield [row[p] if p is not None and p < len(row) else None for p in pos]


# ----------------------------------------------------------------------------- GEO (IBGE)
def _uf_of(m):
    """UF info for a municipality; some new municipalities have no microrregiao, so try both paths."""
    ri = (m.get("regiao-imediata") or {})
    uf = (ri.get("regiao-intermediaria") or {}).get("UF")
    if not uf:
        uf = ((m.get("microrregiao") or {}).get("mesorregiao") or {}).get("UF")
    return uf


def load_geo(conn, s):
    quality = os.getenv("GEO_QUALITY", "intermediaria")
    log.info("IBGE: municipality list")
    muns = s.get(f"{IBGE_LOC}/municipios", timeout=120).json()
    ufs, rows = {}, []
    for m in muns:
        uf = _uf_of(m)
        if not uf:
            log.warning("no UF for municipality %s", m.get("id"))
            continue
        ufs[uf["id"]] = (uf["id"], uf["sigla"], uf["nome"], (uf.get("regiao") or {}).get("nome"))
        ri = m.get("regiao-imediata") or {}
        rint = ri.get("regiao-intermediaria") or {}
        rows.append((m["id"], m["nome"], norm(m["nome"]), uf["sigla"], ri.get("nome"), rint.get("nome")))
    del muns
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO geo.uf (uf_code,sigla,nome,regiao) VALUES (%s,%s,%s,%s)
               ON CONFLICT (uf_code) DO UPDATE SET sigla=EXCLUDED.sigla,nome=EXCLUDED.nome,regiao=EXCLUDED.regiao""",
            list(ufs.values()))
        cur.executemany(
            """INSERT INTO geo.municipio (ibge_code,nome,nome_norm,uf,regiao_imediata,regiao_intermediaria)
               VALUES (%s,%s,%s,%s,%s,%s)
               ON CONFLICT (ibge_code) DO UPDATE SET nome=EXCLUDED.nome,nome_norm=EXCLUDED.nome_norm,uf=EXCLUDED.uf,
                 regiao_imediata=EXCLUDED.regiao_imediata,regiao_intermediaria=EXCLUDED.regiao_intermediaria""",
            rows)
    log.info("loaded %d UFs, %d municipalities", len(ufs), len(rows))

    geom_sql = "ST_Multi(ST_CollectionExtract(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON(%s),4326)),3))"
    params = {"formato": "application/vnd.geo+json", "qualidade": quality}

    def mesh(path, intra):
        r = s.get(f"{IBGE_MESH}/{path}", params={**params, "intrarregiao": intra}, timeout=300)
        r.raise_for_status()
        return r.json()["features"]

    feats = mesh("paises/BR", "UF")
    with conn.transaction(), conn.cursor() as cur:
        for f in feats:
            cur.execute(f"UPDATE geo.uf SET geom={geom_sql} WHERE uf_code=%s",
                        (json.dumps(f["geometry"]), int(f["properties"]["codarea"])))
    del feats

    for sigla, code in sorted(UF_CODES.items()):          # one state at a time keeps RAM small
        feats = mesh(f"estados/{code}", "municipio")
        with conn.transaction(), conn.cursor() as cur:
            for f in feats:
                cur.execute(f"UPDATE geo.municipio SET geom={geom_sql} WHERE ibge_code=%s",
                            (json.dumps(f["geometry"]), int(f["properties"]["codarea"])))
        log.info("geometry %s: %d municipalities (rss %.0f MB)", sigla, len(feats), rss_mb())
        del feats

    ov = os.getenv("TSE_IBGE_OVERRIDES")
    if ov and os.path.exists(ov):
        with open(ov, newline="", encoding="utf-8") as fh, conn.transaction(), conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO geo.tse_ibge_override VALUES (%s,%s) ON CONFLICT (cd_municipio) DO UPDATE SET ibge_code=EXCLUDED.ibge_code",
                [(int(r["cd_municipio"]), int(r["ibge_code"])) for r in csv.DictReader(fh)])


# ----------------------------------------------------------------------------- VOTES (TSE)
def resolve_units(s, scopes, ufs, years_override):
    """For each (scope, UF) pick the newest year whose file exists (2026 first, then fallbacks)."""
    units = []
    for scope in scopes:
        years = [int(y) for y in (years_override or DEFAULT_YEARS[scope]).split(",")]
        for uf in ufs:
            if scope == "municipal" and uf == "ZZ":
                continue
            for year in years:
                url = f"{TSE_BASE}/votacao_secao/votacao_secao_{year}_{uf}.zip"
                meta = probe(s, url)
                if meta:
                    units.append((scope, year, uf, url, meta))
                    log.info("%-9s %s -> %d", scope, uf, year)
                    break
                log.info("%-9s %s: %d not available, falling back", scope, uf, year)
            else:
                log.warning("%s %s: no data in any of %s", scope, uf, years)
    return units


def vote_rows(zip_path, stats):
    cols = ["ANO_ELEICAO", "NR_TURNO", "CD_CARGO", "DS_CARGO", "SG_UF", "CD_MUNICIPIO", "NM_MUNICIPIO",
            "NR_ZONA", "NR_VOTAVEL", "NM_VOTAVEL", "QT_VOTOS", "SQ_CANDIDATO"]
    required = [c for c in cols if c != "SQ_CANDIDATO"]
    cargos, muns = stats["cargos"], stats["muns"]
    for r in iter_zip_csv(zip_path, cols, required):
        try:
            ano, turno, cargo = int(r[0]), int(r[1]), int(r[2])
            mun, zona, nr, qt = int(r[5]), int(r[7]), int(r[8]), int(r[10])
        except (ValueError, TypeError):
            stats["bad"] += 1
            continue
        cargos[cargo] = r[3]
        muns[mun] = (r[4], r[6])
        if nr == 95:
            tipo = "branco"
        elif nr == 96:
            tipo = "nulo"
        elif r[11] is None:
            tipo = "nominal"
        else:   # SQ_CANDIDATO <= 0 / empty means the vote went to a party label, not a candidate
            sq = r[11].strip()
            tipo = "nominal" if sq.lstrip("-").isdigit() and int(sq) > 0 else "legenda"
        stats["rows"] += 1
        yield (ano, turno, cargo, r[4], mun, zona, nr, (r[9] or "").strip(), tipo, qt)


def already_loaded(conn, year, uf, meta):
    row = conn.execute("SELECT etag,last_modified FROM electoral.carga_log WHERE ano=%s AND uf=%s", (year, uf)).fetchone()
    return bool(row and (meta["etag"] or meta["last_modified"]) and row == (meta["etag"], meta["last_modified"]))


def load_unit(conn, s, unit, tmpdir):
    scope, year, uf, url, meta = unit
    tmp = os.path.join(tmpdir, f"tse_{year}_{uf}.zip")
    t0 = time.time()
    download(s, url, tmp)
    stats = {"rows": 0, "bad": 0, "cargos": {}, "muns": {}}
    try:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute("SET LOCAL work_mem = '64MB'")
            cur.execute("TRUNCATE staging.votacao_secao")
            with cur.copy(f"COPY staging.votacao_secao ({STAGE_COLS}) FROM STDIN") as cp:
                for row in vote_rows(tmp, stats):
                    cp.write_row(row)
            cur.executemany("INSERT INTO electoral.cargo VALUES (%s,%s) ON CONFLICT (cd_cargo) DO UPDATE SET nome=EXCLUDED.nome",
                            list(stats["cargos"].items()))
            cur.executemany(
                "INSERT INTO geo.tse_municipio (cd_municipio,uf,nome,nome_norm) VALUES (%s,%s,%s,%s) "
                "ON CONFLICT (cd_municipio) DO UPDATE SET nome=EXCLUDED.nome,nome_norm=EXCLUDED.nome_norm,uf=EXCLUDED.uf",
                [(c, v[0], v[1], norm(v[1])) for c, v in stats["muns"].items()])
            cur.execute("""INSERT INTO electoral.votavel (ano,cd_cargo,nr_votavel,nm_votavel,tipo)
                           SELECT DISTINCT ano,cd_cargo,nr_votavel,nm_votavel,tipo FROM staging.votacao_secao
                           ON CONFLICT DO NOTHING""")
            cur.execute("DELETE FROM electoral.votos_zona WHERE ano=%s AND uf=%s", (year, uf))   # atomic replace
            cur.execute("""INSERT INTO electoral.votos_zona (ano,turno,cd_cargo,uf,cd_municipio,zona,votavel_id,votos)
                           SELECT s.ano,s.turno,s.cd_cargo,s.uf,s.cd_municipio,s.zona,v.id,SUM(s.qt_votos)
                           FROM staging.votacao_secao s
                           JOIN electoral.votavel v ON v.ano=s.ano AND v.cd_cargo=s.cd_cargo
                                AND v.nr_votavel=s.nr_votavel AND v.nm_votavel=s.nm_votavel
                           GROUP BY s.ano,s.turno,s.cd_cargo,s.uf,s.cd_municipio,s.zona,v.id""")
            cur.execute("""INSERT INTO electoral.zona (uf,cd_municipio,zona)
                           SELECT DISTINCT uf,cd_municipio,zona FROM staging.votacao_secao ON CONFLICT DO NOTHING""")
            cur.execute("TRUNCATE staging.votacao_secao")
            cur.execute("""INSERT INTO electoral.carga_log (ano,uf,scope,url,etag,last_modified,linhas)
                           VALUES (%s,%s,%s,%s,%s,%s,%s)
                           ON CONFLICT (ano,uf) DO UPDATE SET scope=EXCLUDED.scope,url=EXCLUDED.url,etag=EXCLUDED.etag,
                             last_modified=EXCLUDED.last_modified,linhas=EXCLUDED.linhas,loaded_at=now()""",
                        (year, uf, scope, url, meta["etag"], meta["last_modified"], stats["rows"]))
    finally:
        os.remove(tmp)
    log.info("%s %s %d: %d rows (%d skipped) in %.0fs, peak rss %.0f MB",
             scope, uf, year, stats["rows"], stats["bad"], time.time() - t0, rss_mb())


def load_votes(conn, s, args, tmpdir):
    ufs = args.uf or (UFS + ["ZZ"])
    units = resolve_units(s, args.scope, ufs, args.years)
    for unit in units:
        if not args.force and already_loaded(conn, unit[1], unit[2], unit[4]):
            log.info("skip %s %d (unchanged)", unit[2], unit[1])
            continue
        load_unit(conn, s, unit, tmpdir)
    log.info("linking TSE -> IBGE municipality codes and refreshing summaries")
    conn.execute("SELECT electoral.link_tse_ibge()")
    conn.execute("SELECT electoral.refresh_summaries()")
    unmatched = conn.execute("SELECT count(*) FROM geo.tse_municipio WHERE ibge_code IS NULL AND uf<>'ZZ'").fetchone()[0]
    if unmatched:
        log.warning("%d TSE municipalities unmatched to IBGE; add rows to TSE_IBGE_OVERRIDES csv and re-run 'geo'+'votes'", unmatched)


# ----------------------------------------------------------------------------- ZONE POINTS (optional)
def load_zones(conn, s, tmpdir):
    """Best effort: zone centroid = mean of its polling-place coordinates. Fails soft."""
    tmpl = os.getenv("TSE_LOCAIS_URL", TSE_BASE + "/eleitorado_locais_votacao/eleitorado_local_votacao_{year}.zip")
    for year in [2026, 2024, 2022, 2020, 2018]:
        url = tmpl.format(year=year)
        if not probe(s, url):
            continue
        tmp = os.path.join(tmpdir, f"locais_{year}.zip")
        download(s, url, tmp)
        try:
            cols = ["SG_UF", "CD_MUNICIPIO", "NR_ZONA", "NR_LATITUDE", "NR_LONGITUDE"]
            with conn.transaction(), conn.cursor() as cur:
                cur.execute("TRUNCATE staging.zona_pt")
                with cur.copy("COPY staging.zona_pt (uf,cd_municipio,zona,lat,lon) FROM STDIN") as cp:
                    for r in iter_zip_csv(tmp, cols, cols):
                        try:
                            cp.write_row((r[0], int(r[1]), int(r[2]),
                                          float(r[3].replace(",", ".")), float(r[4].replace(",", "."))))
                        except (ValueError, AttributeError):
                            continue
                cur.execute("""INSERT INTO electoral.zona (uf,cd_municipio,zona,n_locais,geom)
                               SELECT uf,cd_municipio,zona,count(*),ST_SetSRID(ST_MakePoint(avg(lon),avg(lat)),4326)
                               FROM staging.zona_pt
                               WHERE lat BETWEEN -34 AND 6 AND lon BETWEEN -75 AND -28
                               GROUP BY uf,cd_municipio,zona
                               ON CONFLICT (uf,cd_municipio,zona) DO UPDATE SET n_locais=EXCLUDED.n_locais,geom=EXCLUDED.geom""")
                cur.execute("TRUNCATE staging.zona_pt")
            log.info("zone centroids loaded from %d polling-place file", year)
            return
        finally:
            os.remove(tmp)
    log.warning("no polling-place coordinate file found; zones will have no geometry (filters still work)")


# ----------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["all", "geo", "votes", "zones"])
    ap.add_argument("--scope", nargs="+", choices=["geral", "municipal"], default=["geral", "municipal"])
    ap.add_argument("--uf", nargs="+", help="limit to these UFs (use ZZ for overseas votes)")
    ap.add_argument("--years", help="override year fallback chain, e.g. 2026,2022")
    ap.add_argument("--force", action="store_true", help="reload even if the remote file is unchanged")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    dsn = os.environ.get("LOADER_DATABASE_URL")
    if not dsn:
        sys.exit("set LOADER_DATABASE_URL (see .env.example)")
    s = make_session()
    tmpdir = os.getenv("LOADER_TMPDIR", tempfile.gettempdir())
    with psycopg.connect(dsn, autocommit=True) as conn:
        if args.command in ("all", "geo"):
            load_geo(conn, s)
        if args.command in ("all", "votes"):
            load_votes(conn, s, args, tmpdir)
        if args.command in ("all", "zones"):
            load_zones(conn, s, tmpdir)
    log.info("done. peak RSS %.0f MB", rss_mb())


if __name__ == "__main__":
    main()
