# TSE → PostGIS loader for an interactive Brazil map

## Run it
```bash
mkdir secrets && openssl rand -base64 24 > secrets/pg_superuser_pw.txt
docker compose up -d db
export PGPASSWORD=$(cat secrets/pg_superuser_pw.txt)
psql -h localhost -U postgres -d electoral -v loader_pw="$(openssl rand -base64 24)" -v app_pw="$(openssl rand -base64 24)" -f sql/00_roles.sql
psql -h localhost -U postgres -d electoral -f sql/01_schema.sql

pip install -r requirements.txt
export LOADER_DATABASE_URL='postgresql://electoral_loader:<loader_pw>@localhost:5432/electoral'
python loader.py all            # geo (IBGE) → votes (TSE) → zone points
python loader.py votes --uf SP  # re-run is idempotent; unchanged remote files are skipped
```

## What gets loaded
| Source | Data | Table |
|---|---|---|
| TSE `votacao_secao_{ano}_{UF}.zip` | votes, aggregated section → electoral zone | `electoral.votos_zona` |
| IBGE localidades + malhas | UFs, municipalities, immediate/intermediate regions, polygons | `geo.uf`, `geo.municipio` |
| TSE polling-place coordinates (optional) | zone centroids | `electoral.zona` |

**Year fallback:** per UF, tries 2026 → 2022 → 2018 → 2014 (general elections) and 2024 → 2020 → 2016 → 2012 (municipal), HEAD-probing each file and taking the newest that exists. Each family resolves independently, because 2026 has no municipal races. Override with `--years` or `TSE_YEARS_*`.

**Columns dropped:** section number, polling-place name/address, election type/description codes, generation timestamps, duplicate names. Cargo, candidate and municipality names live in dimension tables instead of being repeated on every row.

## Staying under 1 GB
Downloads stream to disk in 1 MiB chunks; CSVs are read row by row from inside the zip; rows go straight into an UNLOGGED staging table via `COPY`; PostgreSQL does the GROUP BY with `work_mem=64MB` and spills to disk. IBGE geometry is fetched one state at a time. The loader logs peak RSS after every UF (expect roughly 60–120 MB). The compose file caps Postgres at 1 GB with `shared_buffers=256MB`. Staging needs temporary disk (a few GB for large states like SP in municipal years).

## Map-oriented design
- `votos_zona` PK is `(ano, turno, cargo, uf, municipio, zona, votável)`, so state → city → zone filters are index range scans.
- Materialized views pre-aggregate municipality totals, state totals and the per-municipality leader, refreshed `CONCURRENTLY` by `electoral.refresh_summaries()`.
- `api.mvt_municipios(z,x,y,ano,turno,cargo[,uf])` returns Mapbox Vector Tiles with the winner and percentage on every municipality polygon, so the whole country renders in MapLibre/Mapbox without shipping GeoJSON. Geometries are pre-projected to EPSG:3857 with a GiST index.
- "City region" = electoral zone (`api.votos_zona`, `api.zona`). Brazil has no official neighbourhood polygons that match TSE data.

## Security model
| Role | Login | Can do |
|---|---|---|
| `electoral_owner` | no | owns all objects; nothing connects as it |
| `electoral_loader` | yes | DML on geo/electoral/staging tables, run the two maintenance functions; no DDL, no `api` access |
| `electoral_app` | yes | `SELECT` on `api.*` views and execute the tile function only; read-only transactions, 15 s statement timeout, 40 connections |

`PUBLIC` is stripped of database `CONNECT`, schema `CREATE`/`USAGE` and function `EXECUTE`. Views and `SECURITY DEFINER` functions (with a pinned `search_path`) mean the app never touches base tables. Before shipping also: require TLS and `scram-sha-256` in `pg_hba.conf`, restrict by source network, keep passwords in a secret manager, and don't bind 5432 publicly.

## Things to check (I could not run this end to end)
The sandbox I built this in has no network or PostgreSQL, so the CSV parser was tested on synthetic data, but the downloads and SQL were not run against live TSE/IBGE/Postgres. Please verify:
1. **2026 files** exist yet. The runoff is today (25 Oct 2026); if the 2026 files aren't published, the fallback silently uses 2022 and logs it.
2. **Polling-place URL** (`TSE_LOCAIS_URL`) is my best guess; if it 404s the loader warns and zones simply have no geometry.
3. **Candidate vs. party votes** (`tipo`) is inferred from `SQ_CANDIDATO ≤ 0`; blank/null from 95/96. Spot-check one state against TSE's totals.
4. **TSE↔IBGE codes** are matched by normalized name within the UF, then fuzzy (≥0.7 trigram). Review `SELECT * FROM geo.tse_municipio WHERE metodo='fuzzy' OR ibge_code IS NULL;` and fix via the overrides CSV.
5. Per-UF files are assumed to include the presidential race; if presidential rows are missing, add the `_BR` file as a unit.
