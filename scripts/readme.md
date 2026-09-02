# DOCUMENTATION DÉTAILLÉE DU CODE

## TABLE DES MATIÈRES

### merge_signalements.py
1. [Vue d'ensemble](#vue-densemble)
2. [Structure du fichier](#structure-du-fichier)
3. [Explication section par section](#explication-section-par-section)
4. [Flux de données](#flux-de-données)
5. [Concepts clés à retenir](#concepts-clés-à-retenir)
6. [Aide pour modification](#aide-pour-modification)

### Autres scripts
7. [fetch_datex_diro.py](#fetch_datex_diropy)
8. [fetch_vigicrues.py](#fetch_vigicruespy)
9. [fetch_stations_hydro.py](#fetch_stations_hydropy)
10. [stats_prefecture.py](#stats_prefecturepy)


---

## VUE D'ENSEMBLE

### Objectif du script merge_signalements.py

Ce script fusionne les données d'inondations provenant de **7 sources différentes** en un seul GeoJSON standardisé, plus deux exports dérivés (Datex II XML, CSV). Il gère également :
- L'archivage annuel permanent (par signalement)
- La détection des suppressions silencieuses (une source qui retire une entrée sans jamais dire pourquoi)
- Le monitoring de l'état de chaque flux
- Un coupe-circuit par source (`DISABLED_SOURCES`), pour désactiver une source sans toucher au code

C'est un portage Python de l'ancien `grist-to-geojson.js` (Node), remplacé le 03/08/2026. La logique métier (archivage, filtrage, monitoring) est restée la même ; seul le langage a changé.

### Sources de données

1. **Grist 35** : signalements manuels saisis par les agents (formulaire Grist)
2. **CD44** : API REST du département de Loire-Atlantique
3. **Rennes Métropole** : service WFS (Web Feature Service)
4. **CD35 Inondations** : API OGC du département d'Ille-et-Vilaine, signalements ponctuels
5. **CD35 Inondations Linéaire** : API OGC du département d'Ille-et-Vilaine, tronçons linéaires
6. **CD56** : API OGC du département du Morbihan
7. **DIRO** : fichier GeoJSON généré par `fetch_datex_diro.py` (DIR Ouest, Datex II Bison Futé)

### Fichiers générés

```
signalements.geojson              → Tous les signalements actifs fusionnés
signalements_inondation.xml       → Export Datex II v2.3 (mêmes signalements)
signalements_inondation.csv       → Export CSV grand public, sans géométrie
metadata.json                     → Statistiques + monitoring des flux
archives/
  ├── signalements_2025.geojson
  ├── signalements_2026.geojson
  └── last_run.json               → État de la dernière exécution (pour détecter les suppressions)
```

---

## STRUCTURE DU FICHIER

Le code est organisé en sections logiques (bannières `# ===` dans le fichier) :

```
1. CONFIGURATION
   ├─ Identifiants Grist, URLs des API sources
   └─ DISABLED_SOURCES : coupe-circuit par source

2. IDENTIFIANTS UNIQUES
   └─ generate_unique_id()

3. DATES ET HEURES (fuseau Europe/Paris)
   └─ _iso_now(), get_datetime_fr(), format_date(), is_older_than_3_days()

4. PROJECTIONS CARTOGRAPHIQUES
   └─ Lambert 93 / CC48 → WGS84 (pyproj)

5. FILTRAGE DES SIGNALEMENTS RÉSOLUS DEPUIS PLUS DE 3 JOURS
   └─ should_keep_feature()

6. ARCHIVAGE ANNUEL
   ├─ load_archive() / save_archive()
   ├─ load_last_run() / save_last_run()
   ├─ find_in_archive()
   ├─ add_or_update_in_archive()
   └─ detect_deleted_signalements()

7. MONITORING DES FLUX
   └─ flux_monitor, monitor_fetch()

8. RÉCUPÉRATION + CONVERSION PAR SOURCE
   ├─ Rennes Métropole (WFS)
   ├─ CD35 / CD56 (API OGC Feature)
   ├─ Grist 35
   ├─ CD44
   └─ DIRO

9. EXPORT DATEX II
   └─ build_datex2_export()

10. CROISEMENT COMMUNAL (codes INSEE)
    └─ enrich_with_communes()

11. EXPORT CSV
    └─ build_csv_export()

12. FUSION PRINCIPALE
    └─ merge_sources()
```

---

## EXPLICATION SECTION PAR SECTION

### SECTION 1 : CONFIGURATION

```python
GRIST_DOC_ID = os.environ.get("GRIST_DOC_ID")
GRIST_API_KEY = os.environ.get("GRIST_API_KEY")
```
**Pourquoi des variables d'environnement ?** Ce sont des secrets, jamais en dur dans le code. Définies dans GitLab (Settings > CI/CD > Variables), injectées automatiquement dans le job.

```python
KNOWN_SOURCES = {"grist_35", "cd44", "rennes_metropole", "cd35_inondations", "cd35_lineaire", "cd56", "diro"}
DISABLED_SOURCES = {s.strip() for s in os.environ.get("DISABLED_SOURCES", "").split(",") if s.strip()}
```
**Coupe-circuit par source** : si une source envoie des données erronées ou test, ajouter sa clé dans la variable CI/CD `DISABLED_SOURCES` (séparées par des virgules) la désactive sans toucher au code. Elle n'est plus interrogée du tout et disparaît des exports, sans effacer son historique déjà archivé. Vide par défaut = comportement normal, rien ne change.

---

### SECTION 2 : IDENTIFIANTS UNIQUES

```python
_unique_id_counter = itertools.count(1)
def generate_unique_id():
    return next(_unique_id_counter)
```
**Pourquoi ?** Chaque signalement dans le GeoJSON final a besoin d'un `id` unique. Un compteur simple suffit car le script s'exécute de bout en bout à chaque run (pas de parallélisme sur l'écriture).

---

### SECTION 3 : DATES ET HEURES

Trois représentations d'une même date selon l'usage :
- `_iso_now()` : horodatage UTC ISO (équivalent `Date.toISOString()` JS), pour `metadata.json`
- `get_datetime_fr()` : version française lisible (`"17/12/2025 à 15h30"`), pour l'affichage
- `format_date(date_value)` : convertit n'importe quelle date source (string ISO ou timestamp epoch en secondes/millisecondes) vers ce format français

**Piège gardé volontairement** : `is_older_than_3_days()` compare en heure système naïve (comme le faisait le JS d'origine), pas en Europe/Paris explicite. Comportement identique à l'ancien script, pas un bug.

---

### SECTION 4 : PROJECTIONS CARTOGRAPHIQUES

```python
_lambert93_to_wgs84 = pyproj.Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)
_cc48_to_wgs84 = pyproj.Transformer.from_crs("EPSG:3948", "EPSG:4326", always_xy=True)
```
Les administrations stockent parfois leurs coordonnées en mètres (Lambert 93 ou CC48). Le visualiseur a besoin de degrés (WGS84). Seule Rennes Métropole peut renvoyer en CC48 (détecté automatiquement, cf section 8).

---

### SECTION 5 : FILTRAGE DES RÉSOLUS > 3 JOURS

```python
def should_keep_feature(feature):
    # actif → gardé
    # résolu depuis < 3 jours → gardé (utile pour l'affichage "réouverture récente")
    # résolu depuis > 3 jours → filtré
```
**Pourquoi 3 jours ?** Permet au visualiseur d'afficher encore un instant les coupures récemment réouvertes, sans garder indéfiniment du bruit dans `signalements.geojson`.

---

### SECTION 6 : ARCHIVAGE ANNUEL

**Objectif** : garder une trace permanente de chaque signalement, même supprimé (indépendamment de `signalements.geojson`, qui ne montre que l'état courant).

**Détection d'ID réutilisé** (`add_or_update_in_archive`) : certaines sources réutilisent le même `id_source` pour deux événements différents dans le temps. Un nouvel épisode n'est créé que si `date_debut` diffère **et** que l'épisode précédent n'est plus actif ; sinon c'est juste une dérive de la date déclarée par une source sur une même coupure continue, celle-ci pouvant varier légèrement d'un relevé à l'autre.

**Point d'attention** : `find_in_archive()` doit renvoyer la **dernière** entrée correspondant à `(id_source, source)`, pas la première : une comparaison contre un épisode déjà périmé redéclencherait "ID réutilisé" à chaque run, créant un nouveau doublon à chaque passage.

**Détection des suppressions** (`detect_deleted_signalements`) : certaines sources retirent simplement une entrée de leur flux sans jamais fournir de `date_fin`. Cette fonction compare les IDs actifs du run précédent (`archives/last_run.json`) à ceux du run courant ; un ID disparu sans explication est marqué `"Supprimé"` avec `date_suppression` = l'heure où on l'a remarqué.

---

### SECTION 7 : MONITORING DES FLUX

```python
def monitor_fetch(source_name, fetch_fn):
    # chronomètre + capture le résultat de fetch_fn()
    # statut : OK (données) / EMPTY (0 résultat) / ERROR (exception)
```
Wrapper générique autour de chaque fonction de récupération. Alimente `flux_monitor`, publié dans `metadata.json`, ce qui permet de voir en un coup d'œil quelle source est en panne sans éplucher les logs CI.

---

### SECTION 8 : RÉCUPÉRATION + CONVERSION PAR SOURCE

Chaque source a son format d'origine ; chaque `*_to_feature()` le convertit vers le format standard (`properties.route`, `commune`, `statut_actif`, `type_coupure`, `gestionnaire`...).

**Rennes Métropole** : WFS, filtré sur `raison="inondation"`. Détecte automatiquement si les coordonnées sont en CC48 (X > 1000 en valeur absolue) ou déjà en WGS84.

**CD35 / CD56 (`_fetch_ogc_features`)** : API OGC générique (récupère la 1ère collection, puis ses items). `_cd35_properties()` factorise le mapping commun aux deux couches CD35 (ponctuel + linéaire) : même schéma de champs (`route`, `etat_circulation`, `commune`, `agence`, `prd`/`prf`, `Date_saisie` en epoch ms). La casse du champ identifiant peut différer selon la couche (`OBJECTID` ou `objectid`) : les deux sont testés. `etat_circulation` pilote `type_coupure` (Totale/Partielle) par une correspondance simple plutôt qu'une table exhaustive, ce format pouvant encore évoluer.

**Grist 35** : la géométrie peut être un point simple (`Latitude`/`Longitude`) ou du GeoJSON complexe saisi en texte libre (`fields.geojson`, pour une ligne par ex.).

**CD44** : sans identifiant stable disponible, `id_source` est calculé par hash de `route + commune + période de fermeture`, pour éviter que deux coupures distinctes s'écrasent dans l'archive.

**DIRO** : lit un fichier local (`data/inondations-diro.geojson`, déjà généré par `fetch_datex_diro.py`), filtré sur `is_active = true`.

---

### SECTION 9 : EXPORT DATEX II

`build_datex2_export()` reproduit l'intégralité des signalements (pas de filtre par cause, `signalements.geojson` ne contient déjà que des inondations en pratique) au format Datex II v2.3, type `EnvironmentalObstruction`/`flooding`. Structure vérifiée ligne à ligne contre le XSD officiel (validation automatisée disponible via le job CI `validate-datex2`).

Point à retenir si le schéma doit évoluer : `PointByCoordinates` n'hérite pas de `GroupOfLocations` : il doit être imbriqué dans un élément `Point`, qui lui en hérite. `overallStartTime` est obligatoire dans le schéma ; repli sur `date_debut` puis `date_saisie` puis l'heure de génération si aucune date exploitable n'est disponible.

---

### SECTION 10 : CROISEMENT COMMUNAL

`enrich_with_communes()` ajoute `properties.codes_insee` à chaque signalement, par géocodage inverse (API IGN Géoplateforme, même service que la recherche d'adresse du visualiseur), sans BDTopo embarquée ni shapely. Un point = 1 appel ; une ligne = 3 points échantillonnés (début/milieu/fin). Repli sur l'index `parcel` si l'index `address` ne répond rien (cas des routes rurales sans adresse à proximité).

---

### SECTION 11 : EXPORT CSV

`build_csv_export()` écrit en `utf-8-sig` (avec BOM) plutôt qu'`utf-8` simple : sans le BOM, Excel (l'outil le plus probable pour ouvrir un CSV grand public) ne détecte pas l'UTF-8 et affiche les accents cassés.

---

### SECTION 12 : FUSION PRINCIPALE

`merge_sources()` orchestre tout :
1. Récupère les 7 sources en parallèle (`ThreadPoolExecutor`), en sautant celles listées dans `DISABLED_SOURCES`
2. Convertit chaque item brut en feature standard, filtre via `should_keep_feature()`
3. Enrichit avec les codes INSEE
4. Archive chaque feature, détecte les suppressions
5. Écrit `signalements.geojson`, `signalements_inondation.xml`, `signalements_inondation.csv`
6. Écrit `metadata.json` (statistiques + monitoring)

---

## FLUX DE DONNÉES COMPLET

```
1. RÉCUPÉRATION (parallèle, sources non désactivées seulement)
   ├─ Grist API           → N records
   ├─ CD44 API             → N records
   ├─ Rennes WFS            → N records
   ├─ CD35 OGC (ponctuel)   → N records
   ├─ CD35 OGC (linéaire)   → N records
   ├─ CD56 OGC              → N records
   └─ DIRO fichier local    → N records

2. CONVERSION (par source) → format standard uniforme

3. FILTRAGE
   ├─ Garder : actifs
   ├─ Garder : résolus < 3 jours
   └─ Retirer : résolus > 3 jours

4. ENRICHISSEMENT → codes_insee par géocodage inverse

5. ARCHIVAGE
   ├─ Ajouter/mettre à jour dans archives/<année>.geojson
   ├─ Détecter les suppressions silencieuses
   └─ Sauvegarder last_run.json

6. GÉNÉRATION
   ├─ signalements.geojson
   ├─ signalements_inondation.xml (Datex II)
   ├─ signalements_inondation.csv
   └─ metadata.json (stats + monitoring)
```

---

## CONCEPTS CLÉS À RETENIR

### 1. Monitoring vs Données
`flux_monitor`/`metadata.json` décrivent l'état des flux (OK/EMPTY/ERROR) ; `signalements.geojson` contient les données elles-mêmes. Deux choses distinctes.

### 2. `id` vs `id_source`
`id` : généré par nous, unique dans le GeoJSON final. `id_source` : identifiant d'origine de la source (peut être réutilisé par la source elle-même, cf section 6).

### 3. Statut d'un signalement
`statut_actif` : route encore coupée. `statut_resolu` : route réouverte. Un signalement peut être résolu mais encore présent dans le fichier (< 3 jours, cf section 5).

### 4. Conversions de projection
Lambert 93 (EPSG:2154) et CC48 (EPSG:3948) → WGS84 (EPSG:4326). Seule Rennes Métropole peut nécessiter cette conversion aujourd'hui.

### 5. Gestion des erreurs
Chaque fonction `fetch_*` attrape ses propres erreurs réseau et retourne une liste vide en cas d'échec, les autres sources continuent normalement. L'erreur est capturée dans `flux_monitor`, jamais dans les données publiées.

---

## AIDE POUR MODIFICATION

### Ajouter une nouvelle source

1. Ajouter sa clé dans `KNOWN_SOURCES`
2. Écrire `fetch_<source>_data()` (attrape ses propres erreurs, retourne `[]` en cas d'échec)
3. Écrire `<source>_to_feature()` (retourne le format standard, `None` si non convertible)
4. Ajouter dans `flux_monitor` (section 7)
5. Ajouter dans le `ThreadPoolExecutor` de `merge_sources()` (via `_submit_or_skip`)
6. Ajouter dans `stats`, les totaux, `metadata["sources"]`, la docstring en tête de fichier, et `current_actifs` dans `detect_deleted_signalements()`

### Couper une source sans toucher au code

Variable CI/CD `DISABLED_SOURCES`, cf section 1. Voir aussi l'en-tête de [`.gitlab-ci.yml`](../.gitlab-ci.yml).

### Modifier le format de sortie

Modifier les fonctions `*_to_feature()` pour changer le mapping des propriétés.

### Changer le seuil de filtrage (3 jours)

Modifier la constante dans `is_older_than_3_days()` (section 5) : `diff_days > 3`.

### Ajouter un champ à `metadata.json`

Modifier le dict `metadata` en fin de `merge_sources()` (section 12).

---

## fetch_datex_diro.py

### Rôle

Récupère le flux DATEX II (standard européen d'échange de données routières) publié par Bison Futé pour la DIR Ouest, filtre les événements pertinents et les convertit en GeoJSON. C'est ce fichier (`data/inondations-diro.geojson`) que `merge_signalements.py` relit ensuite comme source « DIRO ».

### Fonctionnement

1. `fetch_xml()` télécharge le flux XML brut.
2. `parse_datex()` parcourt chaque `situation`/`situationRecord` et applique trois filtres successifs : source DIR Ouest, type `EnvironmentalObstruction`, sous-type `flooding`/`flashFloods` (avec un repli par mots-clés si le sous-type n'est pas explicite). Calcule aussi si l'événement est encore actif en comparant `overallEndTime` à l'heure courante.
3. `create_geojson()` écrit le GeoJSON ainsi qu'un fichier de statistiques texte (`data/inondations-diro-stats.txt`), utile pour un contrôle visuel rapide du filtrage.

### À retenir

Contrairement à `merge_signalements.py`, ce script conserve aussi bien les événements actifs que terminés dans son fichier de sortie (`is_active` sur chaque feature) : c'est `merge_signalements.py` qui filtre ensuite sur les actifs uniquement (`fetch_diro_data()`, `is_active is True`).

---

## fetch_vigicrues.py

### Rôle

Interroge les flux RSS Vigicrues pour les 7 tronçons du bassin de la Vilaine, calcule le niveau de vigilance maximal parmi eux, et écrit `data/vigilance.json`. Ce fichier alimente le sous-titre du visualiseur et sert de déclencheur à `fetch_stations_hydro.py`.

### Fonctionnement

- `TRONCONS` : correspondance code Vigicrues → nom lisible, figée en dur (ces tronçons ne changent pas).
- `fetch_troncon_niveau()` lit le `<title>` du flux RSS d'un tronçon (ex. `"Vilaine amont : vert"`) et en extrait la couleur.
- `collect_niveaux()` interroge tous les tronçons ; un tronçon en échec n'empêche pas les autres d'être pris en compte (`erreurs` collecté à part).
- Le niveau global retenu est le maximum parmi les tronçons obtenus avec succès.

---

## fetch_stations_hydro.py

### Rôle

Exporte les stations hydrométriques Hub'Eau (35/44/56) et leur dernière hauteur d'eau connue. Deux sorties : un instantané du jour (`stations_hydro.geojson`, écrasé à chaque run, pour l'exploration cartographique) et une accumulation annuelle qui ne s'efface jamais (`hauteurs_eau_<année>.geojson`), en vue d'un futur croisement avec l'historique des coupures.

### Déclenchement conditionnel

Le script ne s'exécute pleinement que si l'un de ces deux critères est vrai :
- la vigilance Vigicrues (lue dans `vigilance.json`) est au moins jaune, **ou**
- au moins une coupure de route active existe (lue dans `signalements.geojson`), une crue locale pouvant couper une route sans faire monter la vigilance des 7 tronçons suivis.

En cas d'erreur de lecture de l'un ou l'autre fichier, le script part du principe qu'il vaut mieux relever par précaution que rater un vrai événement (comportement dit « fail-open »).

### Vigilance par station

`STATION_TRONCON` fait correspondre chaque station à son tronçon Vigicrues, pour lui attacher le niveau de vigilance courant. Cette correspondance est figée en dur plutôt que recalculée à chaque run : elle change rarement, et la reconstruire suppose d'interroger une API Vigicrues non documentée publiquement.

### Fiabilité des relevés

Chaque relevé de hauteur (`fetch_latest_hauteur`) est retenté jusqu'à 3 fois en cas d'échec réseau, avant d'abandonner pour cette station sans bloquer les autres.

---

## stats_prefecture.py

### Rôle

Script indépendant de `merge_signalements.py` : relit `signalements.geojson` déjà publié, agrège les coupures actives par commune, et publie le résultat à deux endroits :
- un document Grist dédié (table `Situation_actuelle`, réécrite à chaque run, et `Historique_signalements`, un historique par point/tronçon) ;
- une page HTML autonome (`stats-prefecture.html`), avec indicateurs clés, répartition par type de route et par organisme, et un tableau détaillé par commune.

### Périmètre géographique

Seuls les signalements dont le point représentatif tombe dans le périmètre défini par `masque.geojson` sont pris en compte, avec le même fichier et la même règle géométrique que le visualiseur cartographique, pour garantir une définition cohérente du périmètre entre les deux.

### Classification des routes

Chaque coupure est classée RN/RD/Autres à partir du nom de route déclaré (reconnaissance par expression régulière, tolérante à un préfixe « R » optionnel selon la source).

### Colonnes par organisme

Les sources institutionnelles (DIRO, Rennes Métropole, CD35, CD44, CD56) ont chacune une colonne fixe. Les signalements saisis manuellement peuvent provenir de plusieurs organismes différents (champ `gestionnaire`) : une colonne est créée dynamiquement par organisme rencontré, plutôt qu'une colonne unique qui les confondrait tous. Ces colonnes sont créées automatiquement dans Grist si elles n'existent pas encore.

### Historique par signalement

`Historique_signalements` identifie chaque coupure par la paire `(source, id_source)`, le seul identifiant stable d'un run à l'autre. Une nouvelle ligne apparaît à l'apparition d'une coupure, et se complète (`date_reouverture`) quand elle n'est plus vue active, sans jamais recréer de doublon pour le même événement.

---




---

FIN DE LA DOCUMENTATION
