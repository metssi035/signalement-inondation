# Sources de données - Signalements Inondations

Documentation technique par gestionnaire des flux de données pour la cartographie des routes coupées par inondations.

---

## Filtre temporel global

Appliqué à toutes les sources après fusion des données.

### Règle
- Signalements actifs : conservés
- Signalements résolus :
  - `date_fin` ≤ 3 jours : conservés
  - `date_fin` > 3 jours : filtrés


## 1. Grist Ille-et-Vilaine (35)

### Source
Saisie manuelle via base de données collaborative Grist.

**URL API**
```
https://grist.dataregion.fr/o/inforoute/api/docs/${GRIST_DOC_ID}/tables/Signalements/records
```

### Projection
WGS84 (EPSG:4326)

### Géométrie
Points, LineStrings, Polygons (mixte selon saisie manuelle)

### Filtres
- Filtre sur valeurs "L" dans le champ `Cause`
- Aucun filtre temporel à la source

### Résultat
3 signalements récupérés, 3 conservés

---

## 2. Conseil Départemental de Loire-Atlantique (44)

### Source
API REST Open Data du Département 44.

**URL API**
```
https://data.loire-atlantique.fr/api/explore/v2.1/catalog/datasets/224400028_info-route-departementale/records?limit=100
```

### Projection
WGS84 (EPSG:4326)

### Géométrie
Points uniquement

### Filtres
```javascript
type.toLowerCase() === 'inondation'
```

### Résultat
19 signalements récupérés, 0 conservés (aucune inondation active)

---

## 3. Rennes Métropole

### Source
Service WFS (Web Feature Service) du SIG de Rennes Métropole.

**URL WFS**
```
https://public.sig.rennesmetropole.fr/geoserver/ows?SERVICE=WFS&REQUEST=GetFeature&VERSION=2.0.0&TYPENAMES=trp_rout:routes_coupees&OUTPUTFORMAT=json
```

### Projection
CC48 (EPSG:3948) converti en WGS84 (EPSG:4326)

Définition CC48 :
```
+proj=lcc +lat_0=48 +lon_0=3 +lat_1=47.25 +lat_2=48.75 
+x_0=1700000 +y_0=7200000 +ellps=GRS80 +units=m
```

### Géométrie
LineStrings, MultiLineStrings (tracés de routes)

### Filtres
```javascript
raison.toLowerCase() === 'inondation'
```
Etat= en cours ou Etat=terminée mais depuis moins de 3 jours.

### Résultat
119 signalements récupérés, 16 conservés

---

## 4. Conseil Départemental d'Ille-et-Vilaine (35)

### Source
Service WFS ArcGIS Server du Département 35.

**URL WFS**
```
https://dservices1.arcgis.com/jGLANYlFVVx3nuxa/arcgis/services/Inondations_cd35/WFSServer?service=WFS&version=2.0.0&request=GetFeature&typeNames=Inondations_cd35:Inondation&srsName=EPSG:2154
```

### Projection
Lambert 93 (EPSG:2154) converti en WGS84 (EPSG:4326)

Définition Lambert 93 :
```
+proj=lcc +lat_1=49 +lat_2=44 +lat_0=46.5 +lon_0=3 
+x_0=700000 +y_0=6600000 +ellps=GRS80 +units=m
```

### Géométrie
Points uniquement

### Filtres
Aucun

### Résultat
0 signalements récupérés - NE FONCTIONNE PAS 

---

## 5. Conseil Départemental du Morbihan (56)

### Source
API OGC Features (REST) ArcGIS du Département 56.

**URL API**
```
https://services.arcgis.com/4GFMPbPboxIs6KOG/arcgis/rest/services/INONDATION/OGCFeatureServer
```

### Projection
WGS84 (EPSG:4326)

### Géométrie
Points, LineStrings, Polygons (mixte)

### Filtres
```javascript
conditions_circulation === 'COUPÉE' || conditions_circulation === 'INONDÉE PARTIELLE'
```
(insensible à la casse)

### Résultat
116 signalements récupérés, 25 conservés

---



---


## Format de sortie

### Fichier principal
`signalements.geojson` - FeatureCollection GeoJSON


### Métadonnées
`metadata.json` - Statistiques et horodatage

---

## Pipeline de mise à jour

### Fréquence
Toutes les 30 minutes


### Archivage
- Système d'archivage annuel automatique
- Détection des suppressions entre exécutions
- Fichiers d'archive horodatés

---

## Dépendances techniques

### Librairies Node.js
- `https` (natif)
- `fs` (natif)
- `node-fetch`
- `xml2js`
- `proj4`
