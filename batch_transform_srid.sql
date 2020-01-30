-- This script aims at converting the real geometry columns to another srid. 

-- this will be done on a backup database, where we drop all dependencies to tables (views and triggers)
-- and then generate SQL to transform and declare geometry columns to new type. 

-- not to be used in production, this DESTROYS the database structure. It is aimed to be used for doing a dataonly dump and then fill in a raw database structure. 




 create table  qwat_od_temp as (SELECT * FROM "qwat_od"."vw_pipe_schema_visibleitems" )
 
 update "qwat_od"."vw_pipe_schema_visibleitems" set geometry = st_setsrid("geometry" , 21781 )
  
 
 -- drop views
 
 -- drop all triggers
 
 select distinct(trigger_name), * from information_schema.triggers where trigger_schema in ('qwat_od', 'qwat_dr', 'qwat_sys', 'qwat_vl');
 
CREATE OR REPLACE FUNCTION strip_all_triggers() RETURNS text AS $$ DECLARE
    triggNameRecord RECORD;
    triggTableRecord RECORD;
BEGIN
    FOR triggNameRecord IN select distinct(trigger_name) from information_schema.triggers where trigger_schema in ('qwat_od', 'qwat_dr', 'qwat_sys', 'qwat_vl') LOOP
        FOR triggTableRecord IN SELECT distinct event_object_table, event_object_schema  from information_schema.triggers where trigger_name = triggNameRecord.trigger_name LOOP
            RAISE NOTICE 'Dropping trigger: % on table: %', triggNameRecord.trigger_name, triggTableRecord.event_object_table;
            EXECUTE 'DROP TRIGGER ' || triggNameRecord.trigger_name || ' ON ' || triggTableRecord.event_object_schema || '.' || triggTableRecord.event_object_table || ';';
        END LOOP;
    END LOOP;

    RETURN 'done';
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

select strip_all_triggers();
 
 
 -- alter table using st_tranform
 
 ALTER TABLE qwat_od.pressurezone
 ALTER COLUMN geometry TYPE geometry(MultiPolygon, 3946) 
  USING ST_Transform(geometry, 3946);
 
 select f_table_schema, f_table_name, f_geometry_column, type , * from public.geometry_columns;

select st_geometrytype(st_force3D(geometry)), st_asconstructionpoint_fk_object_referenceewkt(st_force3D(geometry)), st_zmflag(geometry) from qwat_dr.constructionpoint
 
---- create more complete geometry_column view with full typemod

drop view if exists geometry_columns_detail ;
create view geometry_columns_detail as (
 SELECT current_database()::character varying(256) AS f_table_catalog,
    n.nspname AS f_table_schema,
    c.relname AS f_table_name,
    a.attname AS f_geometry_column,
    COALESCE(postgis_typmod_dims(a.atttypmod), sn.ndims, 2) AS coord_dimension,
    COALESCE(NULLIF(postgis_typmod_srid(a.atttypmod), 0), sr.srid, 0) AS srid,
    replace(replace(COALESCE(NULLIF(upper(postgis_typmod_type(a.atttypmod)), 'GEOMETRY'::text), st.type, 'GEOMETRY'::text), 'ZM'::text, ''::text), 'Z'::text, ''::text)::character varying(30) AS type, 
	postgis_typmod_type(a.atttypmod) as typemod
   FROM pg_class c
     JOIN pg_attribute a ON a.attrelid = c.oid AND NOT a.attisdropped
     JOIN pg_namespace n ON c.relnamespace = n.oid
     JOIN pg_type t ON a.atttypid = t.oid
     LEFT JOIN ( SELECT s.connamespace,
            s.conrelid,
            s.conkey,
            replace(split_part(s.consrc, ''''::text, 2), ')'::text, ''::text) AS type
           FROM ( SELECT pg_constraint.connamespace,
                    pg_constraint.conrelid,
                    pg_constraint.conkey,
                    pg_get_constraintdef(pg_constraint.oid) AS consrc
                   FROM pg_constraint) s
          WHERE s.consrc ~~* '%geometrytype(% = %'::text) st ON st.connamespace = n.oid AND st.conrelid = c.oid AND (a.attnum = ANY (st.conkey))
     LEFT JOIN ( SELECT s.connamespace,
            s.conrelid,
            s.conkey,
            replace(split_part(s.consrc, ' = '::text, 2), ')'::text, ''::text)::integer AS ndims
           FROM ( SELECT pg_constraint.connamespace,
                    pg_constraint.conrelid,
                    pg_constraint.conkey,
                    pg_get_constraintdef(pg_constraint.oid) AS consrc
                   FROM pg_constraint) s
          WHERE s.consrc ~~* '%ndims(% = %'::text) sn ON sn.connamespace = n.oid AND sn.conrelid = c.oid AND (a.attnum = ANY (sn.conkey))
     LEFT JOIN ( SELECT s.connamespace,
            s.conrelid,
            s.conkey,
            replace(replace(split_part(s.consrc, ' = '::text, 2), ')'::text, ''::text), '('::text, ''::text)::integer AS srid
           FROM ( SELECT pg_constraint.connamespace,
                    pg_constraint.conrelid,
                    pg_constraint.conkey,
                    pg_get_constraintdef(pg_constraint.oid) AS consrc
                   FROM pg_constraint) s
          WHERE s.consrc ~~* '%srid(% = %'::text) sr ON sr.connamespace = n.oid AND sr.conrelid = c.oid AND (a.attnum = ANY (sr.conkey))
  WHERE (c.relkind = ANY (ARRAY['r'::"char", 'v'::"char", 'm'::"char", 'f'::"char", 'p'::"char"])) AND NOT c.relname = 'raster_columns'::name AND t.typname = 'geometry'::name AND NOT pg_is_other_temp_schema(c.relnamespace) AND has_table_privilege(c.oid, 'SELECT'::text)
)
;
 
CREATE OR REPLACE FUNCTION convert_all_geometries() RETURNS text AS $$ DECLARE
    geomRecord RECORD;
    
BEGIN
    FOR geomRecord IN  select f_table_schema as sname, f_table_name as tname, f_geometry_column as geomname, typemod as geomtype   from public.geometry_columns_detail LOOP
        RAISE NOTICE 'Transforming column: % on table: %', geomRecord.geomname, geomRecord.tname;
        EXECUTE 'ALTER TABLE  ' || geomRecord.sname || '.' || geomRecord.tname|| ' ALTER COLUMN ' || 
			geomRecord.geomname || ' TYPE geometry(' || geomRecord.geomtype || ', 3946' || ') USING ST_Transform(' || geomRecord.geomname || ', 3946);' ;
        
    END LOOP;

    RETURN 'done';
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

select convert_all_geometries();
 
 ALTER TABLE  qwat_dr.constructionpoint ALTER COLUMN geometry TYPE geometry(POINTZ, 3946) USING ST_Transform(geometry, 3946);
 
 
 -- problème pour récupérer le type réel avec typemod. Essai en passant par le type géométrique système

 
 -- translate to charente
 
 
 -- export data
 
 