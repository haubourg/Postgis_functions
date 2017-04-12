# Postgis_functions
EN: various generic psql functions made in postgis

List of available functions:

- intersect_layers:
auteur(es) / Author : Régis Haubourg - Agence de l'eau Adour Garonne

[EN]: A generic function made to calculate overlapping area and ratios easy between polygon layers or polygon*line layers. 


[FR]: Fonction permettant de simplifier les opération de croisement et calcul de recouvrement entre couches de polygones * Polygone ou  Lignes * Polygones
calcule l'intersection avec un seuil de tolérance de nettoyage des micros polygones, agrége les relations sur la base des clés primaires de chaque table pour chaque couple unique. 
Offre également un filtre SQL pour chaque table source si besoin. Accepte les clause WHERE et LIMIT. 


USAGE :
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

```
select * from  services.intersect_layers(''ref.admin_commune_ag'', ''insee_commune'', ''geoml93'', ''WHERE insee_commune like ''''31%'''''', ''zon.zpf_zos'', ''code_zpf'', ''geoml93'', NULL, 50 );
```
