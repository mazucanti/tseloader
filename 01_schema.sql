-- Run as superuser AFTER 00_roles.sql:  psql -d electoral -f sql/01_schema.sql
\set ON_ERROR_STOP on

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE SCHEMA IF NOT EXISTS geo      AUTHORIZATION electoral_owner;
CREATE SCHEMA IF NOT EXISTS electoral AUTHORIZATION electoral_owner;
CREATE SCHEMA IF NOT EXISTS staging  AUTHORIZATION electoral_owner;
CREATE SCHEMA IF NOT EXISTS api      AUTHORIZATION electoral_owner;

SET ROLE electoral_owner;   -- everything below is owned by the non-login owner role

-------------------------------------------------------------------------------
-- GEO (IBGE)
-------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS geo.uf (
  uf_code    smallint PRIMARY KEY,
  sigla      char(2)  NOT NULL UNIQUE,
  nome       text     NOT NULL,
  regiao     text,
  geom       geometry(MultiPolygon, 4326),
  geom_3857  geometry(MultiPolygon, 3857) GENERATED ALWAYS AS (ST_Transform(geom, 3857)) STORED
);

CREATE TABLE IF NOT EXISTS geo.municipio (
  ibge_code                int PRIMARY KEY,
  nome                     text NOT NULL,
  nome_norm                text NOT NULL,
  uf                       char(2) NOT NULL REFERENCES geo.uf(sigla),
  regiao_imediata          text,
  regiao_intermediaria     text,
  geom                     geometry(MultiPolygon, 4326),
  geom_3857                geometry(MultiPolygon, 3857) GENERATED ALWAYS AS (ST_Transform(geom, 3857)) STORED
);
CREATE INDEX IF NOT EXISTS municipio_uf_idx      ON geo.municipio (uf, nome);
CREATE INDEX IF NOT EXISTS municipio_norm_trgm   ON geo.municipio USING gin (nome_norm gin_trgm_ops);
CREATE INDEX IF NOT EXISTS municipio_geom3857_gi ON geo.municipio USING gist (geom_3857);

-- TSE municipality codes differ from IBGE codes -> crosswalk
CREATE TABLE IF NOT EXISTS geo.tse_municipio (
  cd_municipio int PRIMARY KEY,
  uf           char(2) NOT NULL,
  nome         text NOT NULL,
  nome_norm    text NOT NULL,
  ibge_code    int REFERENCES geo.municipio(ibge_code),
  metodo       text                       -- override | exact | fuzzy | NULL (unmatched / abroad)
);
CREATE INDEX IF NOT EXISTS tse_mun_ibge_idx ON geo.tse_municipio (ibge_code);
CREATE TABLE IF NOT EXISTS geo.tse_ibge_override (
  cd_municipio int PRIMARY KEY,
  ibge_code    int NOT NULL
);

-------------------------------------------------------------------------------
-- ELECTORAL (TSE) - only the columns the map needs
-------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS electoral.cargo (
  cd_cargo smallint PRIMARY KEY,
  nome     text NOT NULL
);

CREATE TABLE IF NOT EXISTS electoral.votavel (       -- candidate / party / blank / null
  id         integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  ano        smallint NOT NULL,
  cd_cargo   smallint NOT NULL,
  nr_votavel integer  NOT NULL,
  nm_votavel text     NOT NULL,
  tipo       text     NOT NULL CHECK (tipo IN ('nominal','legenda','branco','nulo')),
  UNIQUE (ano, cd_cargo, nr_votavel, nm_votavel)
);

-- Grain: election-year / round / office / state / city / electoral zone / votable.
-- Section-level detail is aggregated away (the biggest size win).
CREATE TABLE IF NOT EXISTS electoral.votos_zona (
  ano          smallint NOT NULL,
  turno        smallint NOT NULL,
  cd_cargo     smallint NOT NULL,
  uf           char(2)  NOT NULL,
  cd_municipio int      NOT NULL,
  zona         smallint NOT NULL,
  votavel_id   integer  NOT NULL REFERENCES electoral.votavel(id),
  votos        integer  NOT NULL,
  PRIMARY KEY (ano, turno, cd_cargo, uf, cd_municipio, zona, votavel_id)  -- serves state -> city -> zone drill-down
) WITH (fillfactor = 100);
CREATE INDEX IF NOT EXISTS votos_zona_votavel_idx ON electoral.votos_zona (votavel_id, ano, turno);
CREATE INDEX IF NOT EXISTS votos_zona_load_idx    ON electoral.votos_zona (ano, uf);

-- "City region" = electoral zone (+ optional centroid from polling-place coordinates)
CREATE TABLE IF NOT EXISTS electoral.zona (
  uf           char(2)  NOT NULL,
  cd_municipio int      NOT NULL,
  zona         smallint NOT NULL,
  n_locais     integer,
  geom         geometry(Point, 4326),
  PRIMARY KEY (uf, cd_municipio, zona)
);
CREATE INDEX IF NOT EXISTS zona_geom_gi ON electoral.zona USING gist (geom);

CREATE TABLE IF NOT EXISTS electoral.carga_log (
  ano           smallint NOT NULL,
  uf            char(2)  NOT NULL,
  scope         text     NOT NULL,
  url           text     NOT NULL,
  etag          text,
  last_modified text,
  linhas        bigint,
  loaded_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (ano, uf)
);

-- Scratch tables for streaming COPY (UNLOGGED = no WAL, truncated after each unit)
CREATE UNLOGGED TABLE IF NOT EXISTS staging.votacao_secao (
  ano smallint, turno smallint, cd_cargo smallint, uf char(2), cd_municipio int,
  zona smallint, nr_votavel int, nm_votavel text, tipo text, qt_votos int
);
CREATE UNLOGGED TABLE IF NOT EXISTS staging.zona_pt (
  uf char(2), cd_municipio int, zona smallint, lat double precision, lon double precision
);

-------------------------------------------------------------------------------
-- Pre-aggregated summaries for the map
-------------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS electoral.mv_votos_municipio AS
SELECT v.ano, v.turno, v.cd_cargo, v.uf, t.ibge_code, v.votavel_id, SUM(v.votos)::bigint AS votos
FROM electoral.votos_zona v
JOIN geo.tse_municipio t ON t.cd_municipio = v.cd_municipio
WHERE t.ibge_code IS NOT NULL
GROUP BY v.ano, v.turno, v.cd_cargo, v.uf, t.ibge_code, v.votavel_id
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS mv_vm_pk ON electoral.mv_votos_municipio (ano, turno, cd_cargo, ibge_code, votavel_id);
CREATE INDEX IF NOT EXISTS mv_vm_uf ON electoral.mv_votos_municipio (ano, turno, cd_cargo, uf);

CREATE MATERIALIZED VIEW IF NOT EXISTS electoral.mv_votos_uf AS
SELECT ano, turno, cd_cargo, uf, votavel_id, SUM(votos)::bigint AS votos
FROM electoral.votos_zona
GROUP BY ano, turno, cd_cargo, uf, votavel_id
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS mv_vuf_pk ON electoral.mv_votos_uf (ano, turno, cd_cargo, uf, votavel_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS electoral.mv_vencedor_municipio AS
WITH base AS (
  SELECT m.ano, m.turno, m.cd_cargo, m.uf, m.ibge_code, m.votavel_id, m.votos, vt.tipo,
         SUM(m.votos) FILTER (WHERE vt.tipo IN ('nominal','legenda'))
           OVER (PARTITION BY m.ano, m.turno, m.cd_cargo, m.ibge_code) AS validos
  FROM electoral.mv_votos_municipio m
  JOIN electoral.votavel vt ON vt.id = m.votavel_id
)
SELECT DISTINCT ON (ano, turno, cd_cargo, ibge_code)
       ano, turno, cd_cargo, uf, ibge_code, votavel_id, votos, validos,
       ROUND(100.0 * votos / NULLIF(validos, 0), 2) AS pct
FROM base
WHERE tipo IN ('nominal','legenda')
ORDER BY ano, turno, cd_cargo, ibge_code, votos DESC
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS mv_ven_pk ON electoral.mv_vencedor_municipio (ano, turno, cd_cargo, ibge_code);
CREATE INDEX IF NOT EXISTS mv_ven_uf ON electoral.mv_vencedor_municipio (ano, turno, cd_cargo, uf);

-------------------------------------------------------------------------------
-- Maintenance functions (SECURITY DEFINER: loader needs no DDL / matview ownership)
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION electoral.link_tse_ibge() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public SET work_mem = '128MB' AS $$
BEGIN
  UPDATE geo.tse_municipio t SET ibge_code = o.ibge_code, metodo = 'override'
  FROM geo.tse_ibge_override o
  WHERE o.cd_municipio = t.cd_municipio AND EXISTS (SELECT 1 FROM geo.municipio m WHERE m.ibge_code = o.ibge_code);

  UPDATE geo.tse_municipio t SET ibge_code = m.ibge_code, metodo = 'exact'
  FROM geo.municipio m
  WHERE t.ibge_code IS NULL AND t.uf <> 'ZZ' AND m.uf = t.uf AND m.nome_norm = t.nome_norm;

  UPDATE geo.tse_municipio t SET ibge_code = x.ibge_code, metodo = 'fuzzy'
  FROM (
    SELECT DISTINCT ON (t2.cd_municipio) t2.cd_municipio, m.ibge_code
    FROM geo.tse_municipio t2
    JOIN geo.municipio m ON m.uf = t2.uf AND similarity(m.nome_norm, t2.nome_norm) >= 0.7
    WHERE t2.ibge_code IS NULL AND t2.uf <> 'ZZ'
      AND NOT EXISTS (SELECT 1 FROM geo.tse_municipio o WHERE o.ibge_code = m.ibge_code)
    ORDER BY t2.cd_municipio, similarity(m.nome_norm, t2.nome_norm) DESC
  ) x
  WHERE t.cd_municipio = x.cd_municipio;
END $$;

CREATE OR REPLACE FUNCTION electoral.refresh_summaries() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public SET work_mem = '128MB' AS $$
DECLARE mv text;
BEGIN
  FOREACH mv IN ARRAY ARRAY['mv_votos_municipio','mv_votos_uf','mv_vencedor_municipio'] LOOP
    IF (SELECT relispopulated FROM pg_class WHERE oid = ('electoral.' || mv)::regclass) THEN
      EXECUTE format('REFRESH MATERIALIZED VIEW CONCURRENTLY electoral.%I', mv);
    ELSE
      EXECUTE format('REFRESH MATERIALIZED VIEW electoral.%I', mv);
    END IF;
  END LOOP;
  ANALYZE electoral.votos_zona;
END $$;

REVOKE EXECUTE ON FUNCTION electoral.link_tse_ibge(), electoral.refresh_summaries() FROM PUBLIC;

-------------------------------------------------------------------------------
-- API schema: the ONLY thing the app role can touch
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW api.uf AS
  SELECT uf_code, sigla, nome, regiao, geom FROM geo.uf;

CREATE OR REPLACE VIEW api.municipio AS
  SELECT ibge_code, nome, uf, regiao_imediata, regiao_intermediaria, geom FROM geo.municipio;

CREATE OR REPLACE VIEW api.cargo AS SELECT cd_cargo, nome FROM electoral.cargo;

CREATE OR REPLACE VIEW api.eleicao_disponivel AS
  SELECT DISTINCT u.ano, u.turno, u.cd_cargo, c.nome AS cargo, u.uf
  FROM electoral.mv_votos_uf u JOIN electoral.cargo c USING (cd_cargo);

CREATE OR REPLACE VIEW api.votos_uf AS
  SELECT u.ano, u.turno, u.cd_cargo, u.uf, v.nr_votavel, v.nm_votavel, v.tipo, u.votos
  FROM electoral.mv_votos_uf u JOIN electoral.votavel v ON v.id = u.votavel_id;

CREATE OR REPLACE VIEW api.votos_municipio AS
  SELECT m.ano, m.turno, m.cd_cargo, m.uf, m.ibge_code, v.nr_votavel, v.nm_votavel, v.tipo, m.votos
  FROM electoral.mv_votos_municipio m JOIN electoral.votavel v ON v.id = m.votavel_id;

CREATE OR REPLACE VIEW api.votos_zona AS
  SELECT z.ano, z.turno, z.cd_cargo, z.uf, t.ibge_code, z.cd_municipio AS cd_municipio_tse, z.zona,
         v.nr_votavel, v.nm_votavel, v.tipo, z.votos
  FROM electoral.votos_zona z
  JOIN electoral.votavel v ON v.id = z.votavel_id
  LEFT JOIN geo.tse_municipio t ON t.cd_municipio = z.cd_municipio;

CREATE OR REPLACE VIEW api.zona AS
  SELECT z.uf, t.ibge_code, z.cd_municipio AS cd_municipio_tse, z.zona, z.n_locais, z.geom
  FROM electoral.zona z LEFT JOIN geo.tse_municipio t ON t.cd_municipio = z.cd_municipio;

CREATE OR REPLACE VIEW api.vencedor_municipio AS
  SELECT w.ano, w.turno, w.cd_cargo, w.uf, w.ibge_code, v.nr_votavel, v.nm_votavel,
         w.votos, w.validos, w.pct
  FROM electoral.mv_vencedor_municipio w JOIN electoral.votavel v ON v.id = w.votavel_id;

-- Vector tiles (MVT) for the whole-country choropleth: /tiles/{z}/{x}/{y}?ano=&turno=&cargo=&uf=
CREATE OR REPLACE FUNCTION api.mvt_municipios(
  z integer, x integer, y integer,
  p_ano smallint, p_turno smallint, p_cargo smallint, p_uf char(2) DEFAULT NULL
) RETURNS bytea
LANGUAGE sql STABLE PARALLEL SAFE SECURITY DEFINER
SET search_path = pg_catalog, public AS $$
  WITH env AS (SELECT ST_TileEnvelope(z, x, y) AS e),
  g AS (
    SELECT m.ibge_code, m.nome, m.uf, vt.nr_votavel, vt.nm_votavel, w.votos, w.pct,
           ST_AsMVTGeom(m.geom_3857, env.e, 4096, 64, true) AS geom
    FROM geo.municipio m
    CROSS JOIN env
    LEFT JOIN electoral.mv_vencedor_municipio w
           ON w.ibge_code = m.ibge_code AND w.ano = p_ano AND w.turno = p_turno AND w.cd_cargo = p_cargo
    LEFT JOIN electoral.votavel vt ON vt.id = w.votavel_id
    WHERE z BETWEEN 0 AND 22 AND m.geom_3857 && env.e
      AND (p_uf IS NULL OR m.uf = p_uf)
  )
  SELECT COALESCE(ST_AsMVT(g.*, 'municipios', 4096, 'geom'), ''::bytea) FROM g WHERE g.geom IS NOT NULL;
$$;

-------------------------------------------------------------------------------
-- Grants (least privilege)
-------------------------------------------------------------------------------
RESET ROLE;
REVOKE ALL ON SCHEMA geo, electoral, staging, api FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA geo, electoral, staging, api FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION api.mvt_municipios(integer,integer,integer,smallint,smallint,smallint,char) FROM PUBLIC;

-- loader: write data, no DDL, no access to api
GRANT USAGE ON SCHEMA geo, electoral, staging TO electoral_loader;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA geo TO electoral_loader;
GRANT SELECT, INSERT, UPDATE, DELETE ON electoral.cargo, electoral.votavel, electoral.votos_zona,
      electoral.zona, electoral.carga_log TO electoral_loader;
GRANT ALL ON ALL TABLES IN SCHEMA staging TO electoral_loader;
GRANT EXECUTE ON FUNCTION electoral.link_tse_ibge(), electoral.refresh_summaries() TO electoral_loader;

-- app: read-only through api views / tile function only
GRANT USAGE ON SCHEMA api TO electoral_app;
GRANT SELECT ON ALL TABLES IN SCHEMA api TO electoral_app;
GRANT EXECUTE ON FUNCTION api.mvt_municipios(integer,integer,integer,smallint,smallint,smallint,char) TO electoral_app;
