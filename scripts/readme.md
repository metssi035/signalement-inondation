# 📚 DOCUMENTATION DÉTAILLÉE DU CODE
# Script : grist-to-geojson__monitoring-only.js

## 📋 TABLE DES MATIÈRES
1. [Vue d'ensemble](#vue-densemble)
2. [Structure du fichier](#structure-du-fichier)
3. [Explication section par section](#explication-section-par-section)
4. [Flux de données](#flux-de-données)
5. [Fonctions principales](#fonctions-principales)

---

## VUE D'ENSEMBLE

### Objectif du script
Ce script fusionne les données d'inondations provenant de **6 sources différentes** en un seul fichier GeoJSON standardisé. Il gère également :
- L'archivage annuel permanent
- La détection des suppressions
- Le monitoring de l'état de chaque flux

### Sources de données
1. **Grist 35** : Signalements manuels saisis par les agents
2. **CD44** : API REST du département de Loire-Atlantique
3. **Rennes Métropole** : Service WFS (Web Feature Service)
4. **CD35** : API OGC du département d'Ille-et-Vilaine
5. **CD56** : API OGC du département du Morbihan
6. **DIRO** : Fichier GeoJSON généré par script Python (DIR Ouest)

### Fichiers générés
```
signalements.geojson     → Tous les signalements actifs fusionnés
metadata.json            → Statistiques + monitoring des flux
archives/
  ├── signalements_2024.geojson
  ├── signalements_2025.geojson
  └── last_run.json
```

---

## 📁 STRUCTURE DU FICHIER

Le code est organisé en sections logiques :

```
1. IMPORTS (lignes 1-40)
   └─ Modules nécessaires : https, fs, fetch, xml2js, proj4

2. CONFIGURATION (lignes 41-100)
   ├─ Projections cartographiques
   ├─ Variables d'environnement
   └─ Chemins des fichiers

3. UTILITAIRES (lignes 101-500)
   ├─ Génération d'IDs uniques
   ├─ Gestion des dates
   ├─ Vérification ancienneté (> 3 jours)
   └─ Filtrage des signalements

4. ARCHIVAGE (lignes 501-800)
   ├─ Chargement/sauvegarde archives
   ├─ Gestion last_run.json
   ├─ Détection des suppressions
   └─ Mise à jour des statuts

5. MONITORING (lignes 801-900)
   ├─ Structure fluxMonitor
   ├─ Wrapper monitorFetch
   └─ Calcul des statuts

6. RÉCUPÉRATION DES DONNÉES (lignes 901-1300)
   ├─ fetchGristData()
   ├─ fetchCD44Data()
   ├─ fetchRennesMetroData()
   ├─ fetchCD35InondationsData()
   ├─ fetchCD56Data()
   └─ fetchDiroData()

7. CONVERSION (lignes 1301-1500)
   ├─ gristToFeature()
   ├─ cd44ToFeature()
   ├─ rennesMetroToFeature()
   ├─ cd35InondationsToFeature()
   ├─ cd56ToFeature()
   └─ diroToFeature()

8. FUSION PRINCIPALE (lignes 1501-fin)
   └─ mergeSources()
```

---

## 📖 EXPLICATION SECTION PAR SECTION

### SECTION 1 : IMPORTS

```javascript
const https = require('https');
```
**Pourquoi ?** Module natif Node.js pour faire des requêtes HTTPS. Utilisé pour Grist car c'est le plus simple pour leur API.

```javascript
const fetch = require('node-fetch');
```
**Pourquoi ?** Alternative moderne à `https`. Utilisé pour les API REST (CD44, CD56, CD35, Rennes).

```javascript
const xml2js = require('xml2js');
```
**Pourquoi ?** Rennes Métropole utilise un service WFS qui retourne du XML. On doit le convertir en JSON.

```javascript
const proj4 = require('proj4');
```
**Pourquoi ?** Les coordonnées arrivent en Lambert 93 ou CC48. On doit tout convertir en WGS84 (latitude/longitude).

---

### SECTION 2 : PROJECTIONS CARTOGRAPHIQUES

```javascript
proj4.defs("EPSG:2154", "...");
```

**Qu'est-ce que c'est ?**
- EPSG:2154 = Lambert 93 (système officiel français)
- Les administrations stockent souvent leurs coordonnées en mètres (X, Y)
- Nous devons tout convertir en degrés (latitude, longitude) pour le web

**Exemple de conversion :**
```
Entrée :  X=359000, Y=6789000 (Lambert 93)
Sortie :  lon=-1.6778, lat=48.1119 (WGS84)
```

---

### SECTION 3 : GÉNÉRATION D'IDs UNIQUES

```javascript
let uniqueIdCounter = 1;
function generateUniqueId() {
    return uniqueIdCounter++;
}
```

**Pourquoi ?**
- Chaque signalement dans le GeoJSON final doit avoir un ID unique
- Le compteur commence à 1 et s'incrémente à chaque nouveau signalement
- Simple mais efficace car le script s'exécute de bout en bout

---

### SECTION 4 : GESTION DES DATES

```javascript
function getDateTimeFR() {
    // ...
    return {
        iso: "2025-12-17T14:30:00.000Z",      // Pour machines
        local: "17/12/2025 à 15h30",          // Pour humains
        timezone: "Europe/Paris"
    };
}
```

**Pourquoi 2 formats ?**
- **ISO** : Standard international, utilisé pour trier/comparer les dates
- **Local** : Format français lisible pour l'affichage

**Attention au fuseau horaire !**
- Les API retournent souvent en UTC
- On convertit tout en heure française pour cohérence

---

### SECTION 5 : FILTRAGE DES SIGNALEMENTS RÉSOLUS

```javascript
function isOlderThan3Days(dateString) {
    // Parse "17/12/2025 à 15h30"
    // Compare avec maintenant
    // Retourne true si > 3 jours
}
```

**Pourquoi filtrer ?**
- On veut garder les signalements actifs
- On veut aussi garder les signalements résolus récents (< 3 jours)
- Mais on retire ceux résolus depuis > 3 jours (plus pertinents)

**Cas d'usage :**
```
Signalement résolu le 10/12 → Aujourd'hui 17/12 → Filtré (7 jours)
Signalement résolu le 15/12 → Aujourd'hui 17/12 → Gardé (2 jours)
```

---

### SECTION 6 : SYSTÈME D'ARCHIVAGE

#### A) Pourquoi archiver ?

**Objectif** : Garder une trace permanente de tous les signalements, même supprimés.

**Structure :**
```
archives/
├── signalements_2024.geojson   → Tout ce qui a commencé en 2024
├── signalements_2025.geojson   → Tout ce qui a commencé en 2025
└── last_run.json                → État de la dernière exécution
```

#### B) Fonctions d'archivage

```javascript
function loadArchive(year) {
    // Charge archives/signalements_2024.geojson
    // Si le fichier n'existe pas, en crée un vide
}
```

```javascript
function saveArchive(year, geojson) {
    // Sauvegarde dans archives/signalements_2024.geojson
    // Met à jour le timestamp last_update
}
```

#### C) Détection des ID réutilisés

**Problème** : Certaines API réutilisent les mêmes IDs pour différents événements !

**Solution** :
```javascript
// On compare AUSSI la date_debut, pas seulement l'id_source
if (existingProps.date_debut !== props.date_debut) {
    // C'est un NOUVEAU signalement avec le même ID !
    // → Créer une nouvelle entrée
} else {
    // C'est vraiment le même signalement
    // → Mettre à jour
}
```

**Exemple concret :**
```
Archive : { id_source: "123", date_debut: "10/12/2025" }
Nouveau : { id_source: "123", date_debut: "15/12/2025" }
→ Ce sont 2 événements différents ! On garde les 2.
```

#### D) Détection des suppressions

```javascript
function detectDeletedSignalements(currentFeatures) {
    // 1. Charge last_run.json (liste des IDs actifs lors de la dernière exécution)
    // 2. Compare avec les IDs actifs maintenant
    // 3. Si un ID était actif avant mais ne l'est plus → Signalement supprimé
    // 4. Marque le signalement comme "Supprimé" dans l'archive
    // 5. Ajoute une date_suppression
}
```

**Cas d'usage :**
```
Exécution N-1 (hier) :  IDs actifs = [123, 456, 789]
Exécution N (aujourd'hui) : IDs actifs = [123, 789]
→ ID 456 a disparu → On le marque "Supprimé" dans l'archive
```

---

### SECTION 7 : MONITORING DES FLUX

#### A) Structure fluxMonitor

```javascript
const fluxMonitor = {
    grist_35: null,              // Sera rempli après le fetch
    cd44: null,
    rennes_metropole: null,
    cd35_inondations: null,
    cd56: null,
    diro: null
};
```

**Rôle** : Stocker l'état de chaque source après récupération.

#### B) Wrapper monitorFetch

```javascript
async function monitorFetch(sourceName, fetchFunction) {
    // 1. Chronomètre le temps de réponse
    const startTime = Date.now();
    
    try {
        // 2. Appelle la fonction de fetch (ex: fetchCD35InondationsData)
        const data = await fetchFunction();
        
        // 3. Calcule le statut
        if (data.length === 0) {
            status = 'EMPTY';  // API fonctionne mais 0 résultat
        } else {
            status = 'OK';      // API fonctionne avec des données
        }
    } catch (error) {
        status = 'ERROR';       // API cassée
    }
    
    // 4. Sauvegarde dans fluxMonitor
    fluxMonitor[sourceName] = status;
    
    // 5. Retourne les données normalement
    return data;
}
```

**Pourquoi ce wrapper ?**
- Permet de surveiller chaque source sans modifier leur code
- Capture les erreurs de façon centralisée
- Mesure les performances (temps de réponse)

#### C) Les 3 statuts possibles

| Statut | Signification | Exemple |
|--------|---------------|---------|
| **OK** | API fonctionne + données disponibles | 10 inondations récupérées |
| **EMPTY** | API fonctionne + 0 résultat | Pas d'inondation active (normal) |
| **ERROR** | API cassée | HTTP 503, timeout, erreur de parsing |

---

### SECTION 8 : RÉCUPÉRATION DES DONNÉES

#### A) Grist (API REST avec authentification)

```javascript
async function fetchGristData() {
    // 1. Configure la requête HTTPS avec authentification Bearer
    const options = {
        hostname: 'grist.dataregion.fr',
        path: `/o/inforoute/api/docs/${GRIST_DOC_ID}/tables/${TABLE_ID}/records`,
        headers: {
            'Authorization': `Bearer ${GRIST_API_KEY}`
        }
    };
    
    // 2. Fait la requête
    // 3. Parse le JSON
    // 4. Retourne records[]
}
```

**Format retourné :**
```javascript
[
    {
        id: 1,
        fields: {
            Latitude: 48.1119,
            Longitude: -1.6778,
            Route: "D137",
            Cause: ["Inondation"],
            ...
        }
    },
    ...
]
```

#### B) CD44 (API REST publique)

```javascript
async function fetchCD44Data() {
    // Appelle : data.loire-atlantique.fr/api/explore/v2.1/...
    // Filtre : Seulement type="Inondation"
    // Retourne : Liste de records avec lat/lon
}
```

#### C) Rennes Métropole (WFS - Web Feature Service)

```javascript
async function fetchRennesMetroData() {
    // 1. Appelle le service WFS
    const url = 'https://public.sig.rennesmetropole.fr/geoserver/ows?SERVICE=WFS...';
    
    // 2. Reçoit du GeoJSON directement
    const geojson = await response.json();
    
    // 3. Filtre uniquement raison="inondation"
    const filtered = geojson.features.filter(f => 
        f.properties.raison.toLowerCase().includes('inondation')
    );
    
    // 4. Détecte si conversion de projection nécessaire
    // 5. Retourne { features: [...], needsConversion: true/false }
}
```

**Particularité** : Rennes peut retourner en CC48 ou WGS84. On détecte automatiquement.

#### D) CD35 (API OGC Feature)

```javascript
async function fetchCD35InondationsData() {
    // 1. Récupère la liste des collections
    const collections = await fetch('.../collections?f=json');
    
    // 2. Prend la première collection (ou cherche "Inondation")
    const collectionId = collections[0].id;
    
    // 3. Récupère les items
    const items = await fetch(`.../collections/${collectionId}/items?f=json`);
    
    // 4. Retourne items.features (déjà en WGS84)
}
```

**Avantage API OGC** : Standard moderne, retourne directement en GeoJSON WGS84.

#### E) CD56 (API OGC Feature)

Identique à CD35, même logique.

#### F) DIRO (Lecture fichier local)

```javascript
async function fetchDiroData() {
    // 1. Vérifie si data/inondations-diro.geojson existe
    if (!fs.existsSync(DIRO_FILE_PATH)) {
        return [];
    }
    
    // 2. Lit le fichier
    const content = fs.readFileSync(DIRO_FILE_PATH, 'utf8');
    const geojson = JSON.parse(content);
    
    // 3. Filtre uniquement is_active = true
    return geojson.features.filter(f => f.properties.is_active === true);
}
```

---

### SECTION 9 : CONVERSION EN FORMAT STANDARD

Chaque source a son propre format. On doit tout standardiser.

#### Format cible (standard)

```javascript
{
    type: 'Feature',
    geometry: { type: 'Point', coordinates: [lon, lat] },
    properties: {
        id: 123,                    // ID unique généré
        id_source: "456",           // ID dans la source d'origine
        source: "CD35 Inondations", // Nom de la source
        route: "D137",
        commune: "Rennes",
        cause: "Inondation",
        statut: "Actif",            // ou "Résolu"
        statut_actif: true,
        statut_resolu: false,
        type_coupure: "Totale",     // ou "Partielle"
        sens_circulation: "",
        commentaire: "...",
        date_debut: "15/12/2025 à 10h30",
        date_fin: "",
        date_saisie: "15/12/2025 à 10h35",
        date_suppression: "",
        gestionnaire: "CD35"
    }
}
```

#### Exemple : gristToFeature()

```javascript
function gristToFeature(record) {
    // 1. Extraire la géométrie
    let geometry;
    if (record.fields.geojson) {
        // GeoJSON déjà saisi manuellement
        geometry = JSON.parse(record.fields.geojson);
    } else if (record.fields.Latitude && record.fields.Longitude) {
        // Point simple
        geometry = {
            type: 'Point',
            coordinates: [record.fields.Longitude, record.fields.Latitude]
        };
    }
    
    // 2. Mapper les champs Grist → format standard
    return {
        type: 'Feature',
        geometry: geometry,
        properties: {
            id: generateUniqueId(),
            id_source: record.id,
            source: 'Saisie Grist',
            route: record.fields.Route || '',
            commune: record.fields.Commune || '',
            cause: record.fields.Cause?.join(', ') || '',
            // ... etc
        }
    };
}
```

**Particularités par source :**

- **Grist** : Peut avoir du GeoJSON complexe (LineString, Polygon)
- **CD44** : Seulement des Points
- **Rennes** : Peut nécessiter conversion CC48 → WGS84
- **CD35/CD56** : Déjà en WGS84, facile
- **DIRO** : Déjà au bon format

---

### SECTION 10 : FUSION PRINCIPALE

```javascript
async function mergeSources() {
    // ============================================
    // ÉTAPE 1 : RÉCUPÉRATION PARALLÈLE
    // ============================================
    const [grist, cd44, rennes, cd35, cd56] = await Promise.all([
        monitorFetch('grist_35', fetchGristData),
        monitorFetch('cd44', fetchCD44Data),
        monitorFetch('rennes_metropole', fetchRennesMetroData),
        monitorFetch('cd35_inondations', fetchCD35InondationsData),
        monitorFetch('cd56', fetchCD56Data)
    ]);
    
    // DIRO en séquentiel (fichier local, très rapide)
    const diro = await monitorFetch('diro', fetchDiroData);
    
    // ============================================
    // ÉTAPE 2 : CONVERSION + FILTRAGE
    // ============================================
    let features = [];
    
    // Pour chaque source
    grist.forEach(record => {
        // 2.1 Convertir au format standard
        const feature = gristToFeature(record);
        
        // 2.2 Vérifier si on doit le garder
        const result = shouldKeepFeature(feature);
        if (result.keep) {
            features.push(feature);
        }
    });
    
    // ... même chose pour cd44, rennes, cd35, cd56, diro
    
    // ============================================
    // ÉTAPE 3 : ARCHIVAGE
    // ============================================
    features.forEach(feature => {
        addOrUpdateInArchive(feature);
    });
    
    detectDeletedSignalements(features);
    
    // ============================================
    // ÉTAPE 4 : GÉNÉRATION DES FICHIERS
    // ============================================
    
    // 4.1 signalements.geojson
    const geojson = {
        type: 'FeatureCollection',
        features: features
    };
    fs.writeFileSync('signalements.geojson', JSON.stringify(geojson));
    
    // 4.2 metadata.json (avec monitoring intégré)
    const metadata = {
        lastUpdate: ...,
        totalRecus: ...,
        sources_recues: {...},
        geometries: {...},
        administrations: {...},
        archives: {...},
        
        // Monitoring calculé directement
        flux_monitoring: {
            globalStatus: ...,  // OK, DEGRADED, ou CRITICAL
            summary: {...},
            sources: fluxMonitor  // Détails par source
        }
    };
    fs.writeFileSync('metadata.json', JSON.stringify(metadata));
}
```

---

## 🔄 FLUX DE DONNÉES COMPLET

```
1. RÉCUPÉRATION (parallèle)
   ├─ Grist API      → 45 records
   ├─ CD44 API       → 12 records
   ├─ Rennes WFS     → 8 records
   ├─ CD35 OGC       → 10 records
   ├─ CD56 OGC       → 11 records
   └─ DIRO fichier   → 3 records
                        ─────────
                        89 records bruts

2. CONVERSION (séquentiel par source)
   Chaque source → Format standard uniforme
   
3. FILTRAGE
   ├─ Garder : Actifs
   ├─ Garder : Résolus < 3 jours
   └─ Retirer : Résolus > 3 jours
                        ─────────
                        85 records gardés

4. ARCHIVAGE
   ├─ Ajouter/Mettre à jour dans archives/2025.geojson
   ├─ Détecter suppressions
   └─ Sauvegarder last_run.json

5. GÉNÉRATION
   ├─ signalements.geojson (85 features)
   └─ metadata.json (stats + monitoring)
```

---

## 🔧 FONCTIONS UTILITAIRES IMPORTANTES

### convertLambert93ToWGS84(x, y)
Convertit des coordonnées Lambert 93 (mètres) en WGS84 (degrés).

### formatDate(dateValue)
Convertit n'importe quel format de date en "DD/MM/YYYY à HHhMM" français.

### shouldKeepFeature(feature)
Détermine si un signalement doit être gardé selon son statut et sa date.

### parseCD44DateFin(ligne4)
Parse les dates spécifiques au format CD44 (ex: "Du 15/12 au 17/12").

---

## 💾 PERSISTANCE DES DONNÉES

### Fichiers éphémères (recréés à chaque run)
- `signalements.geojson`
- `metadata.json`

### Fichiers permanents (jamais supprimés)
- `archives/signalements_2024.geojson`
- `archives/signalements_2025.geojson`
- `archives/last_run.json`

**Pourquoi cette distinction ?**
- Les fichiers de sortie reflètent l'état ACTUEL
- Les archives gardent l'HISTORIQUE COMPLET

---

## 🎓 CONCEPTS CLÉS À RETENIR

### 1. Monitoring vs Données
- **Monitoring** : État des flux (OK/EMPTY/ERROR)
- **Données** : Signalements d'inondations
- Ce sont deux choses différentes stockées ensemble dans metadata.json

### 2. ID unique vs id_source
- **id** : Généré par nous, unique dans le GeoJSON final
- **id_source** : ID d'origine de la source (peut être réutilisé)

### 3. Statut du signalement
- **statut_actif** : true = route encore coupée
- **statut_resolu** : true = route rouverte
- Un signalement peut être résolu mais encore dans le fichier (< 3 jours)

### 4. Conversions de projection
- Lambert 93 (EPSG:2154) → WGS84 (EPSG:4326)
- CC48 (EPSG:3948) → WGS84 (EPSG:4326)
- Toujours vérifier la projection d'entrée !

### 5. Gestion des erreurs
- Chaque fetch est wrappé dans un try/catch
- Si une source échoue, les autres continuent
- L'erreur est capturée dans le monitoring

---

## 📞 AIDE POUR MODIFICATION

### Ajouter une nouvelle source

1. Créer la fonction de fetch
2. Créer la fonction de conversion
3. Ajouter dans fluxMonitor
4. Ajouter dans Promise.all de mergeSources
5. Ajouter le mapping dans la boucle de conversion

### Modifier le format de sortie

Modifier les fonctions `*ToFeature()` pour changer le mapping des propriétés.

### Changer le seuil de filtrage (3 jours)

Modifier la constante dans `isOlderThan3Days()` :
```javascript
return diffDays > 3;  // Changer 3 par autre valeur
```

### Ajouter un nouveau champ dans metadata

Modifier l'objet `metadata` dans `mergeSources()`.

---

FIN DE LA DOCUMENTATION
