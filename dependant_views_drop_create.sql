
CREATE FUNCTION script_create_dependant_views(_table character varying) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
    AS $$
DECLARE
    r record;
    rr record;
    mat record;
    t text;
    _grant text;
    _group text;
    _user text;
    _public boolean;
    test boolean;
    
BEGIN
	-- select script_create_dependant_views('dce.masse_eau')
	-- select script_create_dependant_views('ouv.groupement')
	-- select script_create_dependant_views('ref.admin_commune')
	
    t = '';
	RAISE INFO 'get_drop_script(%)', _table;   

			FOR r IN
				select distinct first_value(depobjid) over w as depobjid, first_value(depobj) over w as depobj, first_value(depth) over w as depth
				from (
					select distinct depobjid, depobj, depth
					from (
						WITH RECURSIVE search_graph(refobj, depobjid, depobj, type, refname, depth, path, cycle) AS (
								SELECT g.refobj, g.depobjid, g.depobj, g.type, g.refname, 1, ARRAY[ROW(g.refobjid, g.depobjid)], false
								FROM admin._dependances g
								where refobj=_table and g.type = 'R' 
								and refobjid != depobjid
							  UNION ALL
								SELECT g.refobj, g.depobjid, g.depobj, g.type, g.refname, sg.depth + 1,  path || ROW(g.refobjid, g.depobjid), ROW(g.refobjid, g.depobjid) = ANY(path)
								FROM admin._dependances g, search_graph sg	
								WHERE g.refobj = sg.depobj and g.type = 'R' AND NOT cycle
								and g.refobjid != g.depobjid
						)
						SELECT * FROM search_graph 
					) x 
				) y  
				window w as (partition by depobj order by depth asc)
				order by depth asc
			LOOP
				
				RAISE INFO 'dépendance : %', r.depobj;   
			
				FOR rr IN
					SELECT c.oid, c.xmin, ns.nspname, c.relname, pg_get_userbyid(c.relowner) AS viewowner, c.relkind, c.relacl, description, pg_get_viewdef(c.oid, true) AS definition
					FROM pg_class c
					LEFT OUTER JOIN pg_description des ON (des.objoid=c.oid and des.objsubid=0)
					LEFT JOIN pg_namespace ns ON ns.oid = c.relnamespace

					WHERE ((c.relhasrules AND (EXISTS (
					SELECT rul.rulename 
					FROM pg_rewrite rul
					WHERE ((rul.ev_class = c.oid)
					AND (bpchar(rul.ev_type) = '1'::bpchar)) ))) OR (c.relkind = 'v'::char) OR (c.relkind = 'm'::char))
					AND c.oid=r.depobjid::oid 
				LOOP
					if rr.relkind = 'v' then 
						RAISE INFO 'create view : %, %', r.depobj, rr.description;   
						t = t || E'-- VIEW ' || r.depobj || E' ----------------------\n\n';
						t = t || 'CREATE OR REPLACE VIEW ' || (regexp_split_to_array(r.depobj, E'\\.'))[1] || '."' || (regexp_split_to_array(r.depobj, E'\\.'))[2] || '"' || E' AS\n';
						t = t || rr.definition || E' \n\n';
	
						if rr.description is not null then
							t = t || 'COMMENT ON VIEW ' || (regexp_split_to_array(r.depobj, E'\\.'))[1] || '."' || (regexp_split_to_array(r.depobj, E'\\.'))[2] || '"' || ' IS ''' || replace(rr.description, '''', '''''') || E''';\n';
						end if;

						t = t || E'\nALTER TABLE ' || (regexp_split_to_array(r.depobj, E'\\.'))[1] || '."' || (regexp_split_to_array(r.depobj, E'\\.'))[2] || '"' || ' OWNER TO '|| rr.viewowner  || E';\n';
						if t is null then RAISE INFO '!!!!!!! script is null !!!!!!!'; end if;
					end if;

					if rr.relkind = 'm' then 
						RAISE INFO 'create materialized view : %, %', r.depobj, rr.description;   
						t = t || E'-- MATERIALIZED VIEW ' || r.depobj || E' -------------------------\n\n';
						t = t || 'CREATE MATERIALIZED VIEW ' || (regexp_split_to_array(r.depobj, E'\\.'))[1] || '."' || (regexp_split_to_array(r.depobj, E'\\.'))[2] || '"' || E' AS\n';
						t = t || rr.definition || E' \n\n';
	
						if rr.description is not null then
							t = t || 'COMMENT ON MATERIALIZED VIEW ' || (regexp_split_to_array(r.depobj, E'\\.'))[1] || '."' || (regexp_split_to_array(r.depobj, E'\\.'))[2] || '"' || ' IS ''' || replace(rr.description, '''', '''''') || E''';\n';
						end if;

						t = t || E'\nALTER TABLE ' || (regexp_split_to_array(r.depobj, E'\\.'))[1] || '."' || (regexp_split_to_array(r.depobj, E'\\.'))[2] || '"' || ' OWNER TO '|| rr.viewowner  || E';\n\n';
						if t is null then RAISE INFO '!!!!!!! script is null !!!!!!!'; end if;


						t = t || E'-- Indexes\n';
						FOR mat IN
							SELECT pg_class.oid,
							    pg_class.relname AS table_name,
							    ns.nspname AS schema_name,
							    pg_class.relkind,
							    i.indexdef
							   FROM pg_class
							     JOIN pg_namespace ns ON ns.oid = pg_class.relnamespace
							     LEFT JOIN pg_indexes i ON i.schemaname = ns.nspname AND i.tablename = pg_class.relname
							  WHERE pg_class.relname !~ '^pg_'::text AND pg_class.relkind = 'm'::"char" 
							  AND (pg_class.relnamespace IN ( SELECT pg_namespace.oid
								   FROM pg_namespace
								  WHERE (pg_namespace.nspname <> ALL (ARRAY['information_schema'::name, 'pg_catalog'::name])) AND pg_namespace.nspname !~~ 'pg_temp%'::text AND pg_namespace.nspname !~~ 'pg_toast%'::text)) AND (pg_class.relname <> ALL (ARRAY['spatial_ref_sys'::name, 'raster_columns'::name, 'raster_overviews'::name]))
							  AND pg_class.oid = rr.oid
							  and i.indexdef is not null
							  ORDER BY ns.nspname, pg_class.relname
						loop
							t = t || mat.indexdef || E';\n';
						end loop;						
					end if;
					t = t || E'\n';
				END LOOP;


				t = t || E'-- Droits\n';
				FOR rr IN
					SELECT c.oid, unnest(c.relacl)::varchar droits
					FROM pg_class c
					LEFT OUTER JOIN pg_description des ON (des.objoid=c.oid and des.objsubid=0)
					WHERE ((c.relhasrules AND (EXISTS (
					SELECT rul.rulename FROM pg_rewrite rul
					WHERE ((rul.ev_class = c.oid)
					AND (bpchar(rul.ev_type) = '1'::bpchar)) ))) OR (c.relkind = 'v'::char) OR (c.relkind = 'm'::char))
					AND c.oid=r.depobjid::oid ORDER BY relname
				LOOP
					RAISE INFO 'Droits à décoder : %', rr.droits;   

					select null into _grant;
					SELECT rr.droits ~ '^=.*' into _public;
					SELECT SUBSTRING(rr.droits, '^group (.+)=.*') into _group;
					SELECT SUBSTRING(rr.droits, '^(.+)=.*') into _user;

					IF _public THEN
						RAISE INFO '  -> public';   
						
						select rr.droits ~ '^=arwdDxt/.*' into test;
						if test THEN
							_grant = 'ALL' ; 
						ELSE
							select rr.droits ~ '^=.*[r].*/.*' into test;
							if test THEN _grant = (case when _grant is null then '' else _grant||', ' end) || 'SELECT' ; END IF;

							select rr.droits ~ '^=.*[a].*/.*' into test;
							if test THEN _grant = (case when _grant is null then '' else _grant||', ' end) || 'INSERT' ; END IF;

							select rr.droits ~ '.+=.*[w].*' into test;
							if test THEN _grant = (case when _grant is null then '' else _grant||', ' end) || 'UPDATE' ; END IF;

							select rr.droits ~ '.+=.*[d].*' into test;
							if test THEN _grant = (case when _grant is null then '' else _grant||', ' end) || 'DELETE' ; END IF;

							select rr.droits ~ '.+=.*[R].*' into test;
							if test THEN _grant = (case when _grant is null then '' else _grant||', ' end) || 'RULE' ; END IF;
						END IF;

						IF _grant is not null THEN
							t = t || E'GRANT ' || _grant || E' ON TABLE '|| (regexp_split_to_array(r.depobj, E'\\.'))[1] || '."' || (regexp_split_to_array(r.depobj, E'\\.'))[2] || '"'  || E' TO PUBLIC ;\n';
							if t is null then RAISE INFO '!!!!!!! script is null !!!!!!!'; end if;
						END IF;
					ELSEIF _group is not null THEN
						RAISE INFO '  -> group';   
						
						select rr.droits ~ '^.+=arwdDxt/.*' into test;
						if test THEN
							_grant = 'ALL' ; 
						ELSE
							select rr.droits ~ '.+=.*[r].*' into test;
							if test THEN _grant = (case when _grant is null then '' else _grant||', ' end) || 'SELECT' ; END IF;

							select rr.droits ~ '.+=.*[a].*' into test;
							if test THEN _grant = (case when _grant is null then '' else _grant||', ' end) || 'INSERT' ; END IF;

							select rr.droits ~ '.+=.*[w].*' into test;
							if test THEN _grant = (case when _grant is null then '' else _grant||', ' end) || 'UPDATE' ; END IF;

							select rr.droits ~ '.+=.*[d].*' into test;
							if test THEN _grant = (case when _grant is null then '' else _grant||', ' end) || 'DELETE' ; END IF;

							select rr.droits ~ '.+=.*[R].*' into test;
							if test THEN _grant = (case when _grant is null then '' else _grant||', ' end) || 'RULE' ; END IF;
						END IF;
						IF _grant is not null THEN
							t = t || E'GRANT ' || _grant || E' ON TABLE '|| (regexp_split_to_array(r.depobj, E'\\.'))[1] || '."' || (regexp_split_to_array(r.depobj, E'\\.'))[2] || '"'  || E' TO GROUP ' || _group || E';\n';
							if t is null then RAISE INFO '!!!!!!! script is null !!!!!!!'; end if;
						END IF;
					ELSEIF _user is not null THEN
						RAISE INFO '  -> user';   
						
						select rr.droits ~ '^.+=arwdDxt/.*' into test;
						if test THEN
							_grant = 'ALL' ; 
						ELSE
							select rr.droits ~ '.+=.*[r].*' into test;
							if test THEN _grant = (case when _grant is null then '' else _grant||', ' end) || 'SELECT' ; END IF;

							select rr.droits ~ '.+=.*[a].*' into test;
							if test THEN _grant = (case when _grant is null then '' else _grant||', ' end) || 'INSERT' ; END IF;

							select rr.droits ~ '.+=.*[w].*' into test;
							if test THEN _grant = (case when _grant is null then '' else _grant||', ' end) || 'UPDATE' ; END IF;

							select rr.droits ~ '.+=.*[d].*' into test;
							if test THEN _grant = (case when _grant is null then '' else _grant||', ' end) || 'DELETE' ; END IF;

							select rr.droits ~ '.+=.*[R].*' into test;
							if test THEN _grant = (case when _grant is null then '' else _grant||', ' end) || 'RULE' ; END IF;
						END IF;

						IF _grant is not null THEN
							t = t || E'GRANT ' || _grant || E' ON TABLE '|| (regexp_split_to_array(r.depobj, E'\\.'))[1] || '."' || (regexp_split_to_array(r.depobj, E'\\.'))[2] || '"'  || E' TO ' || _user || E';\n';
							if t is null then RAISE INFO '!!!!!!! script is null !!!!!!!'; end if;
						END IF;
					END IF;

				END LOOP;

				t = t || E'\n\n';

				t = t || E'-- Commentaires\n';

				FOR rr IN
					SELECT att.*, def.*, pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS defval, CASE WHEN att.attndims > 0 THEN 1 ELSE 0 END AS isarray, format_type(ty.oid,NULL) AS typname, format_type(ty.oid,att.atttypmod) AS displaytypname, tn.nspname as typnspname, et.typname as elemtypname,
					ty.typstorage AS defaultstorage, cl.relname, na.nspname, att.attstattarget, description, cs.relname AS sername, ns.nspname AS serschema,
					(SELECT count(1) FROM pg_type t2 WHERE t2.typname=ty.typname) > 1 AS isdup, indkey,
					CASE 
					WHEN EXISTS( SELECT inhparent FROM pg_inherits WHERE inhrelid=att.attrelid )
					THEN att.attrelid::regclass
					ELSE NULL
					END AS inhrelname,
					attoptions,
					EXISTS(SELECT 1 FROM  pg_constraint WHERE conrelid=att.attrelid AND contype='f' AND att.attnum=ANY(conkey)) As isfk
					FROM pg_attribute att
					JOIN pg_type ty ON ty.oid=atttypid
					JOIN pg_namespace tn ON tn.oid=ty.typnamespace
					JOIN pg_class cl ON cl.oid=att.attrelid
					JOIN pg_namespace na ON na.oid=cl.relnamespace
					LEFT OUTER JOIN pg_type et ON et.oid=ty.typelem
					LEFT OUTER JOIN pg_attrdef def ON adrelid=att.attrelid AND adnum=att.attnum
					LEFT OUTER JOIN pg_description des ON des.objoid=att.attrelid AND des.objsubid=att.attnum
					LEFT OUTER JOIN (pg_depend JOIN pg_class cs ON objid=cs.oid AND cs.relkind='S') ON refobjid=att.attrelid AND refobjsubid=att.attnum
					LEFT OUTER JOIN pg_namespace ns ON ns.oid=cs.relnamespace
					LEFT OUTER JOIN pg_index pi ON pi.indrelid=att.attrelid AND indisprimary
					WHERE att.attrelid = r.depobjid::oid 
					AND att.attisdropped IS FALSE
					and description is not null
					ORDER BY att.attnum				
				LOOP
					t = t || 'COMMENT ON COLUMN ' || (regexp_split_to_array(r.depobj, E'\\.'))[1] || '."' || (regexp_split_to_array(r.depobj, E'\\.'))[2] || '"' || '."' || rr.attname || '" IS '''|| coalesce(replace(rr.description, '''', ''''''), '') || E'''; \n';
					if t is null then RAISE INFO '!!!!!!! script is null !!!!!!!'; end if;
				END LOOP;

				FOR rr IN
					SELECT rw.oid, rw.*, relname, CASE WHEN relkind = 'r' THEN TRUE ELSE FALSE END AS parentistable, nspname, description,
					pg_get_ruledef(rw.oid, true) AS definition
					FROM pg_rewrite rw
					JOIN pg_class cl ON cl.oid=rw.ev_class
					JOIN pg_namespace nsp ON nsp.oid=cl.relnamespace
					LEFT OUTER JOIN pg_description des ON des.objoid=rw.oid
					WHERE ev_class = r.depobjid::oid 
					and rulename != '_RETURN'
					ORDER BY rw.rulename				
				LOOP
					t = t || replace(rr.definition, '''', '''''') || E' \n';
					if t is null then RAISE INFO '!!!!!!! script is null !!!!!!!'; end if;
				END LOOP;

				t = t || E'\n\n';

			END LOOP;

					
RETURN t;
	
END

$$;


ALTER FUNCTION script_create_dependant_views(_table character varying) OWNER TO postgres;

--
-- TOC entry 8321 (class 0 OID 0)
-- Dependencies: 967
-- Name: FUNCTION script_create_dependant_views(_table character varying); Type: COMMENT; Schema: services; Owner: postgres
--

COMMENT ON FUNCTION script_create_dependant_views(_table character varying) IS 'Fonction permettant de générer les scripts de définition des vues faisant référence à une table d''origne, dans l''ordre de création obligatoire. 

Cas d''utilisation: une table est verrouillée en structure par des vues.
1- générer la liste des vues à supprimer pour modifier la table.  script_drop_dependant_views(matbale)
2 - générer le script des création de vues avec script_create_dependant_views(matable)
3 - préparer un script ordonnée des drops / modifs / create 
';


--
-- TOC entry 968 (class 1255 OID 3398600)
-- Name: script_drop_dependant_views(character varying); Type: FUNCTION; Schema: services; Owner: postgres
--

CREATE FUNCTION script_drop_dependant_views(_table character varying) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
    AS $$
DECLARE
    r record;
    t text;
    
BEGIN
  -- test case: 
	-- select script_drop_dependant_views('dce.masse_eau')
  
    t = '';
	RAISE INFO 'get_drop_script(%)', _table;   

			FOR r IN
				select distinct first_value(depobjid) over w as depobjid, 
					first_value(depobj) over w as depobj, 
					first_value(deptyp) over w as deptyp, 
					first_value(depth) over w as depth
				from (
					select distinct depobjid, depobj, deptyp, depth
					from (
						WITH RECURSIVE search_graph(refobj, depobjid, depobj, deptyp, type, refname, depth, path, cycle) AS (
								SELECT g.refobj, g.depobjid, g.depobj, g.deptyp, g.type, g.refname, 1, ARRAY[ROW(g.refobjid, g.depobjid)], false
								FROM admin._dependances g
								where refobj= _table and g.type = 'R' 
								and refobjid != depobjid
							  UNION ALL
								SELECT g.refobj, g.depobjid, g.depobj, g.deptyp, g.type, g.refname, sg.depth + 1,  path || ROW(g.refobjid, g.depobjid), ROW(g.refobjid, g.depobjid) = ANY(path)
								FROM admin._dependances g, search_graph sg	
								WHERE g.refobj = sg.depobj and g.type = 'R' AND NOT cycle
								and g.refobjid != g.depobjid
						)
						SELECT * FROM search_graph 
					) x 
				) y  
				window w as (partition by depobj order by depth desc)
				order by depth desc
				
			LOOP
				
				RAISE INFO 'dépendance : %', r.depobj;   
				if r.deptyp = 'v'::char then
					t = t || 'DROP VIEW ' || (regexp_split_to_array(r.depobj, E'\\.'))[1] || '."' || (regexp_split_to_array(r.depobj, E'\\.'))[2] || '"' || E';\n';
				end if;
				if r.deptyp = 'm'::char then
					t = t || 'DROP MATERIALIZED VIEW ' || (regexp_split_to_array(r.depobj, E'\\.'))[1] || '."' || (regexp_split_to_array(r.depobj, E'\\.'))[2] || '"' || E';\n';
				end if;
			
			END LOOP;
					
RETURN t;
	
END

$$;


--
-- TOC entry 8322 (class 0 OID 0)
-- Dependencies: 968
-- Name: FUNCTION script_drop_dependant_views(_table character varying); Type: COMMENT; Schema: services; Owner: postgres
--

COMMENT ON FUNCTION script_drop_dependant_views(_table character varying) IS 'Cas d''''utilisation: une table est verrouillée en structure par des vues.
1- générer la liste des vues à supprimer pour modifier la table.  script_drop_dependant_views(matbale)
2 - générer le script des création de vues avec script_create_dependant_views(matable)
3 - préparer un script ordonnée des drops / modifs / create 
4- tester en dev  ';


--
