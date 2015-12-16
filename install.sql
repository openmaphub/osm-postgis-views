------------------------------------------------------------------------------------------------------------
-- OSM PostGIS Views
-- like osm2pgsql, but in the same database
-- author: Kristofor Carle (kriscarle)
-- license: MIT
-- Caveats: tags are real-time, but node/way/relations updates require updating the materialized views
-- As a result, this approach is best suited for smaller databases and where updates are relatively infrequent (i.e. not 100s of updates/min)
-- Warning: This has not been fully tested for compatibility with osm2pgsql's data structure
------------------------------------------------------------------------------------------------------------

--HStore and PostGIS extensions required
--CREATE EXTENSION hstore;
--CREATE EXTENSION postgis;

DROP MATERIALIZED VIEW postgis_polygon_geom CASCADE;
DROP MATERIALIZED VIEW postgis_way_geom CASCADE;
DROP MATERIALIZED VIEW postgis_node_geom CASCADE;

--Node Geometry Saved as a Materialized View for Performance
CREATE MATERIALIZED VIEW postgis_node_geom AS
SELECT
id AS node_id,
ST_Transform(ST_SetSRID(ST_MakePoint(longitude::float/10000000,latitude::float/10000000), 4326), 900913)::geometry(POINT,900913) AS geom
FROM current_nodes
WITH DATA
;

CREATE UNIQUE INDEX postgis_node_geom_node_id_idx
  ON postgis_node_geom (node_id);

CREATE INDEX postgis_node_geom_geom_idx ON postgis_node_geom USING GIST (geom);

--Nodes with Tags (matches osm2pgsql table for compatibility
CREATE or replace VIEW planet_osm_nodes AS
SELECT
a.id,
a.latitude as lat,
a.longitude as lon,
CASE WHEN array_agg(b.k::text) = '{NULL}'
THEN null
ELSE hstore(array_agg(b.k::text),array_agg(b.v::text))
END AS tags
FROM current_nodes a
LEFT JOIN current_node_tags b ON a.id = b.node_id
group by a.id
;

--Way Geometry Saved as a Materialized View for Performance
CREATE MATERIALIZED VIEW postgis_way_geom AS
SELECT
a.id AS way_id,
ST_MakeLine(c.geom ORDER BY b.sequence_id)::geometry(LINESTRING,900913) AS geom,
array_agg(b.node_id ORDER BY b.sequence_id) as nodes
FROM current_ways a
LEFT JOIN current_way_nodes b ON a.id = b.way_id
LEFT JOIN postgis_node_geom c ON b.node_id = c.node_id
GROUP BY a.id
WITH DATA
;

CREATE UNIQUE INDEX postgis_way_geom_way_id_idx
  ON postgis_way_geom (way_id);

CREATE INDEX postgis_way_geom_geom_idx ON postgis_way_geom USING GIST (geom);

--Ways (matches osm2pgsql table for compatibility)
CREATE OR REPLACE VIEW planet_osm_ways AS
SELECT
a.way_id as id,
a.nodes,
CASE WHEN count(d.k) = 0
THEN NULL
ELSE hstore(array_agg(d.k::text),array_agg(d.v::text))
END AS tags,
FALSE::BOOLEAN as pending,
a.geom::geometry(LINESTRING,900913) --also adding the geom (not included in osm2pgsql)
FROM postgis_way_geom a
LEFT JOIN current_way_tags d ON a.way_id = d.way_id
GROUP BY a.way_id, a.nodes, a.geom
;

--Select out just line features (aka ways that are not polygons or part of a multipolygon relation)
CREATE OR REPLACE VIEW planet_osm_line AS
SELECT
id as osm_id,
geom::geometry(LINESTRING,900913),
tags
FROM planet_osm_ways
WHERE ((tags->'area') NOT IN ('yes', 'true') OR (tags->'area') IS NULL)
	AND id NOT IN (
    SELECT DISTINCT current_relations.id FROM current_relation_tags
    LEFT JOIN current_relations ON current_relation_tags.relation_id = current_relations.id
    LEFT JOIN current_relation_members  ON current_relation_members.relation_id = current_relations.id
    WHERE k = 'type' AND v = 'multipolygon' AND member_type = 'Way'
  )
;

--Relations
CREATE OR REPLACE VIEW planet_osm_rels AS
SELECT
a.id,
array_agg(b.member_id ORDER BY b.sequence_id) as parts,
array_agg(b.member_type::text ORDER BY b.sequence_id)as types,
array_agg(b.member_role::text ORDER BY b.sequence_id) as roles,
--hstore(, array_agg(b.member_role::text ORDER BY b.sequence_id)) as members_hstore,
hstore(array_agg(c.k::text),array_agg(c.v::text)) AS tags,
FALSE::BOOLEAN as pending
FROM current_relations a
LEFT JOIN current_relation_members b ON a.id = b.relation_id
LEFT JOIN current_relation_tags c ON a.id = c.relation_id
WHERE b.relation_id IS NOT NULL --ignore empty relations, these are just bad data?
GROUP BY a.id
;

--build multipolygons from relations

--Step 1 find relations that have only outer parts, and those with 1 outer and multiple inner (holes)
CREATE OR REPLACE VIEW relation_member_counts AS
SELECT relation_id, member_role, count(member_role)
FROM current_relation_members
WHERE member_type = 'Way'
group by relation_id, member_role
;

--Polygons (aka polygon ways or multipolygon relations)
CREATE MATERIALIZED VIEW postgis_polygon_geom AS
--polygons from ways
SELECT
id AS osm_id,
ST_Multi(ST_MakePolygon(ST_AddPoint(geom, ST_StartPoint(geom))))::geometry(MULTIPOLYGON,900913) AS geom,
'way'::text as osm_source
FROM planet_osm_ways
WHERE (((tags->'area') = 'yes') OR ((tags->'area') = 'true'))

UNION
--Multipolygons
SELECT
a.id as osm_id,
ST_Multi(ST_Union(c.geom ORDER BY b.sequence_id))::geometry(MULTIPOLYGON,900913) as geom,
'rel'::text as osm_source
FROM planet_osm_rels a
LEFT JOIN current_relation_members b ON a.id = b.relation_id
LEFT JOIN (SELECT id, ST_MakeValid(ST_MakePolygon(ST_AddPoint(geom, ST_StartPoint(geom)))) as geom FROM planet_osm_ways) c ON b.member_id = c.id
LEFT JOIN relation_member_counts d ON a.id = d.relation_id
WHERE d.member_role = 'outer' and d.count > 1
GROUP BY a.id

UNION
--Multipolgons with outer + inner holes
SELECT
a.id as osm_id,
CASE WHEN ST_Accum(innerpoly.geom) = '{NULL}'
THEN ST_Multi(ST_MakePolygon(outerpoly.geom))::geometry(MULTIPOLYGON,900913)
ELSE ST_Multi(ST_MakePolygon(outerpoly.geom, ST_Accum(innerpoly.geom order by innerpoly.sequence_id)))::geometry(MULTIPOLYGON,900913)
END AS geom,
'rel'::text as osm_source
FROM planet_osm_rels a
JOIN (
	SELECT b.relation_id,
	CASE WHEN ST_IsClosed(geom) THEN geom
	ELSE ST_AddPoint(geom, ST_StartPoint(geom))
	END as geom
	FROM planet_osm_ways a
	LEFT JOIN current_relation_members b ON a.id = b.member_id
	LEFT JOIN relation_member_counts d ON b.relation_id = d.relation_id
	WHERE b.member_role = 'outer' AND b.member_type = 'Way' AND d.count = 1
) outerpoly ON a.id = outerpoly.relation_id
LEFT JOIN (
	SELECT b.relation_id, a.id as way_id,
	CASE WHEN ST_IsClosed(geom) THEN geom
	ELSE ST_AddPoint(geom, ST_StartPoint(geom))
	END as geom,
	b.sequence_id
	FROM planet_osm_ways a
	LEFT JOIN current_relation_members b ON a.id = b.member_id
	LEFT JOIN relation_member_counts d ON b.relation_id = d.relation_id
	WHERE b.member_role = 'inner' AND  b.member_type = 'Way' AND d.count > 0
) innerpoly ON a.id = innerpoly.relation_id
WHERE ((a.tags->'type') = 'multipolygon')
GROUP BY a.id, outerpoly.geom

WITH DATA
;


CREATE INDEX postgis_polygon_geom_osm_id_idx
  ON postgis_polygon_geom (osm_id);

CREATE INDEX postgis_polygon_geom_geom_idx ON postgis_polygon_geom USING GIST (geom);

--Polygons with Tags
CREATE or replace VIEW planet_osm_polygon AS
SELECT a.osm_id,
a.geom::geometry(MULTIPOLYGON,900913),
b.tags
FROM postgis_polygon_geom a
LEFT JOIN planet_osm_ways b on a.osm_id = b.id
WHERE osm_source = 'way'
UNION
SELECT a.osm_id,
a.geom::geometry(MULTIPOLYGON,900913),
b.tags
FROM postgis_polygon_geom a
LEFT JOIN planet_osm_rels b on a.osm_id = b.id
WHERE osm_source = 'rel';

--POINTS (aka nodes that are not part of lines or polygons)
CREATE OR REPLACE VIEW planet_osm_point AS
SELECT
a.id AS osm_id,
c.geom::geometry(POINT,900913),
a.tags
FROM planet_osm_nodes a
LEFT JOIN current_way_nodes b ON a.id = b.node_id
LEFT JOIN postgis_node_geom c on a.id = c.node_id
WHERE b.node_id IS NULL
AND a.tags IS NOT NULL --ignore orphaned nodes without a way or a tag
;
