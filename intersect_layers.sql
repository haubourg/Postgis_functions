-- Function: intersect_layers(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, integer)

-- DROP FUNCTION intersect_layers(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, integer);

CREATE OR REPLACE FUNCTION intersect_layers(
    IN _taba character varying,
    IN _pkcola character varying,
    IN _geomcola character varying,
    IN _filtertaba character varying,
    IN _tabb character varying,
    IN _pkcolb character varying,
    IN _geomcolb character varying,
    IN _filtertabb character varying,
    IN _cleanthreshold integer)
  RETURNS TABLE("tableA" character varying, "codeTableA" character varying, "tableB" character varying, "codeTableB" character varying, "areaObjectA" double precision, "lengthObjectA" double precision, "areaObjectB" double precision, "lengthObjectB" double precision, "overlapArea" double precision, "overlapLength" double precision, "ratioAinB" double precision, "ratioBinA" double precision) AS
$BODY$

DECLARE
  _query varchar;
   
 
BEGIN
	--variables internes
	IF _cleanThreshold IS NULL THEN 
		_cleanThreshold := 0;
	ELSIF _cleanThreshold <0 THEN  
		_cleanThreshold := 0;
		RAISE EXCEPTION '_cleanThreshold has negative value. restting it to zero';

	END IF;
	
	-- messages debug
	-- call examples:
	
	-- TEST LINESTRING * POLYGON + clauses de filtre + filtre sur les scories d'intersection de moins de 50 m
	--    select * from  intersect_layers('dce.rwbody', 'eu_cd', 'geoml93', 'where version_dce = ''EDL_2013'' limit 150', 'dce.sousbassin_dce', 'eu_cd', 'geoml93', 'where version_dce = ''EDL_2013''', 50 );

	-- TEST POLYGON * POLYGON
	--    select * from  intersect_layers('dce.lwbody', 'eu_cd', 'geoml93', 'where version_dce = ''EDL_2013'' limit 150', 'dce.sousbassin_dce', 'eu_cd', 'geoml93', 'where version_dce = ''EDL_2013''', 50 );


	RAISE NOTICE  'Function aeag_intersect_layers() called :
	 Provides an all-in-one function to relate spatial tables overlaps.
	';



	RAISE INFO 'Parameters input: 
		tabA: "%", tabB:"%",  pkcolumnA: "%" , pkColBcharacter: "%", geomColA: "%", geomColB : "%", filterTabA: "%", filterTabB: "%", cleanThreshold: "%"', _tabA, _tabB, _pkColA, _pkColB, _geomColA, _geomColB, _filterTabA, _filterTabB, _cleanThreshold ;   

	

	--check if tables exist and if exist only once. Does not work for views / to consolidate if schema.table is provided
	-- IF NOT ( (select count(*)  FROM pg_tables WHERE tablename = _tabA)::integer = 1) THEN   
-- 		RAISE EXCEPTION  'Table A "%" does not exists. Escaping function', _tabA;
-- 		RETURN;
-- 		--select intersect_layers('tablenamenotinDB', '_pkColA', '_geomColA', NULL, 'lwbody', '_pkColB', '_geomColB', NULL, 0 );
-- 
-- 	ELSIF NOT ( (select count(*)  FROM pg_tables WHERE tablename = _tabB)::integer = 1) THEN 
-- 		RAISE EXCEPTION 'Table B "%" does not exists. Escaping function', _tabB;
-- 		RETURN;
-- 	ELSIF ((( select count(*)  FROM pg_tables WHERE tablename = _tabA)::integer > 1)  OR  ((select count(*)  FROM pg_tables WHERE tablename = _tabB)::integer > 1)) THEN 
-- 		RAISE EXCEPTION ' Ambiguous table name existing in different schemas' USING HINT = 'Please prefix table name (ex: myschema.mytable)';
-- 		RETURN;
-- 	END IF;
-- 	RAISE INFO 'début du traitement principal ';

	-- check pk column names
	
	-- check geom column names
	
	-- check geom type in geometry_columns
			-- if not declared (postgis <2.0 ) checks all geoms 
			-- if not IN ('POLYGON', MULTIPOLYGON', 'LINESTRING', MULTILINESTRING', 'LINESTRINGM') then raise error + hint
			-- checks if geom are spatially indexed
				-- if not RAISE WARNING but keeps on .
				
	-- starts real work
	
   
   	
   _query := 
		'SELECT tableA, codeTableA, tableB , codeTableB, 
		avg(areaObjectA) areaObjectA, avg(lengthObjectA) lengthObjectA,
		avg(areaObjectB) areaObjectB, avg(lengthObjectB) lengthObjectB,
		sum(overlapArea) overlapArea, sum(overlapLength) overlapLength, 
		CASE WHEN sum(overlapArea) is NULL THEN   NULL ELSE  sum(overlapArea) / avg(areaObjectA) END ratioAinB, 
		CASE WHEN sum(overlapArea) is NULL THEN   NULL ELSE  sum(overlapArea) / avg(areaObjectB) END ratioBinA
		 FROM (
			Select 
				' || quote_literal(_tabA) ||'::character varying tableA, a.code::character varying  codeTableA,
				 b.code::character varying  codeTableB, '||quote_literal(_tabB) ||'::character varying tableB,
				CASE WHEN geometrytype(a.geom) not in (''POLYGON'',''MULTIPOLYGON'') THEN NULL ELSE st_area2d(a.geom) END areaObjectA,
				CASE WHEN geometrytype(a.geom) in (''POLYGON'',''MULTIPOLYGON'') THEN NULL ELSE st_length(a.geom) END lengthObjectA,
				CASE WHEN geometrytype(b.geom) not in (''POLYGON'',''MULTIPOLYGON'') THEN NULL ELSE st_area2d(b.geom) END areaObjectB,
				CASE WHEN geometrytype(b.geom) in (''POLYGON'',''MULTIPOLYGON'') THEN NULL ELSE st_length(b.geom) END lengthObjectB,
				CASE WHEN geometrytype(b.geom) not in (''POLYGON'',''MULTIPOLYGON'') THEN NULL ELSE st_area2d(st_intersection(a.geom, b.geom))  END overlapArea,
				CASE WHEN geometrytype(b.geom) in (''POLYGON'',''MULTIPOLYGON'') AND geometrytype(a.geom) in (''POLYGON'',''MULTIPOLYGON'')THEN NULL ELSE st_length(st_intersection(a.geom, b.geom)) END overlapLength
			FROM
				(select ' ||quote_ident(_pkColA) || '::character varying code, ' || quote_ident(_geomColA) ||' geom from "' || replace(_tabA, '.', '"."') || '" '  || coalesce( _filterTabA,'')  || '  ) a , 
				(select ' ||quote_ident(_pkColB) || '::character varying code, ' || quote_ident(_geomColB) ||' geom from  "' || replace(_tabB, '.', '"."') || '" '|| coalesce( _filterTabB,'')  || ' ) b 
			WHERE
					st_intersects(a.geom, b.geom) 
				AND 
					CASE WHEN (geometrytype(a.geom) in (''POLYGON'',''MULTIPOLYGON'') AND geometrytype(b.geom) in (''POLYGON'',''MULTIPOLYGON'')) THEN 	st_area2d(st_intersection(a.geom, b.geom)) > '||_cleanThreshold||'^2
					ELSE 	st_length(st_intersection(a.geom, b.geom))>'||_cleanThreshold||'   END 
			) as crosselem
		GROUP BY  tableA,  codeTableA, tableB , codeTableB
		 ORDER BY codeTableA, codeTableB' ;

	RAISE INFO 'Requête : %', _query;
	
	RETURN QUERY EXECUTE _query;
	
	RAISE INFO 'fin du traitement principal ';
	RETURN;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 1000
  ROWS 1000;
ALTER FUNCTION intersect_layers(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, integer)
  OWNER TO postgres;
COMMENT ON FUNCTION intersect_layers(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, integer) IS 'fonction de croisement SIG + sortie des ratios de recouvrement entre objets
auteur: RH
date dernière modif: janvier 2012
version: 1.1. modification pour pouvoir utiliser des noms de tables préfixés par le schéma
version : 1.0 première version fonctionnelle
-- tests: géométrie
	-- gerometrytype multipolygon ou polygon / line ou multiline
--entrées / paramètres :
	_taba character varying   - nom de la table A
	_pkcola character varying,  - colonne à utiliser comme clé pour le regroupement des objets (pk)
	_geomcola character varying  - colonne géométrique à utiliser 
	_filtertaba character varying - filtre SQL à inclure, avec le WHERE et/OU LIMIT. Exemple "WHERE monchamp = 22 LIMIT 100"
	_tabb character varying, - nom de la table B
	_pkcolb character varying,  - colonne à utiliser comme clé pour le regroupement des objets (pk)
	_geomcolb character varying, colonne géométrique à utiliser pour la table B
	_filtertabb character varying,  - filtre SQL à inclure, avec le WHERE et/OU LIMIT. Exemple "WHERE monchamp = 22 LIMIT 100"
	_cleanthreshold integer - valeur de tolérance permettant de nettoyer des micro-objets. Valeur en unité du CRS (mètres pour le lambert 93). Pour des croisements ligne X polygone, tous les morceaux de ligne de taille inférieure au seuil sont enlevés. 
	POur des croisements polygon X polygon, les objets de surface inférieur au carré du seuil sont enlevés (pour un seuil de 50m, les objets gardés feront plus de 2500 m2)
 
--exemple d''appel de fonction pour les communes du 31 avec les zos_zpf
select * from  intersect_layers(''ref.admin_commune_ag'', ''insee_commune'', ''geoml93'', ''WHERE insee_commune like ''''31%'''''', ''zon.zpf_zos'', ''code_zpf'', ''geoml93'', NULL, 50 );

-- sorties: une table 
 "tableA " character varying, 
 "codeTableA" character varying, 
 "tableB" character varying, 
 "codeTableB" character varying,
  "areaObjectA" double precision, 
  "lengthObjectA" double precision, 
  "areaObjectB" double precision, 
  "lengthObjectB" double precision,
   "overlapArea" double precision,
    "overlapLength " double precision,
     "ratioAinB" double precision, 
     "ratioBinA" double precision


---messages d''info / exceptions gérées
	-- récap arguments appelé
	-- validité  filtres tables
	-- validité géom
	-- type objets récap
	-- copie de la requête envoyée
	-- récap nombre d''objets tot, agrégés sur code
	-- récap nombre de relation totale, après agrégation couples A-B
	
UNINSTALL script
DROP FUNCTION intersect_layers(_tabA character varying, _pkColA character varying, _geomColA character varying, _filterTabA character varying, _tabB character varying, _pkColB character varying, 
		_geomColB character varying, _filterTabB character varying,	_cleanThreshold integer) ;

---requete modèle non paramétrée:----
---------------------------------------------------------------------------

SELECT tableA, codeTableA , tableB , codeTableB, 
avg(areaObjectA) areaObjectA, avg(lengthObjectA) lengthObjectA,
avg(areaObjectB) areaObjectB, avg(lengthObjectB) lengthObjectB,
sum(overlapArea) overlapArea, sum(overlapLength) overlapLength, 
CASE WHEN sum(overlapArea) is NULL THEN   NULL ELSE  sum(overlapArea) / avg(areaObjectB) END ratioAinB,
CASE WHEN sum(overlapArea) is NULL THEN   NULL ELSE  sum(overlapArea) / avg(areaObjectA) END ratioBinA
 FROM (
	Select 
		''_tabA''::character varying tableA, a.code codeTableA,
		 b.code codeTableB, ''_tabB''::character varying tableB,
		CASE WHEN geometrytype(a.geom) not in (''POLYGON'',''MULTIPOLYGON'') THEN NULL ELSE st_area2d(a.geom) END areaObjectA,
		CASE WHEN geometrytype(a.geom) in (''POLYGON'',''MULTIPOLYGON'') THEN NULL ELSE st_length(a.geom) END lengthObjectA,
		CASE WHEN geometrytype(b.geom) not in (''POLYGON'',''MULTIPOLYGON'') THEN NULL ELSE st_area2d(b.geom) END areaObjectB,
		CASE WHEN geometrytype(b.geom) in (''POLYGON'',''MULTIPOLYGON'') THEN NULL ELSE st_length(b.geom) END lengthObjectB,
		CASE WHEN geometrytype(b.geom) not in (''POLYGON'',''MULTIPOLYGON'') THEN NULL ELSE st_area2d(st_intersection(a.geom, b.geom))  END overlapArea,
		CASE WHEN geometrytype(b.geom) in (''POLYGON'',''MULTIPOLYGON'') THEN NULL ELSE st_length(st_intersection(a.geom, b.geom)) END overlapLength
	FROM
		(select eu_cd code, geom geom from lwbody where eu_cd like ''FRF%'' limit 100 ) a , (select  eu_cd code, geom geom from bvi_grass_cleaned where eu_cd like ''FRF%'' limit 120) b
	WHERE
			st_intersects(a.geom, b.geom) 
		AND 
			CASE WHEN (geometrytype(a.geom) in (''POLYGON'',''MULTIPOLYGON'') AND geometrytype(b.geom) in (''POLYGON'',''MULTIPOLYGON'')) THEN 	st_area2d(st_intersection(a.geom, b.geom)) > 2500 
			ELSE 	st_length(st_intersection(a.geom, b.geom))>50  END 
	) as crosselem
GROUP BY  tableA, codeTableA , tableB , codeTableB
ORDER BY  tableA, codeTableA, tableB , codeTableB
';
