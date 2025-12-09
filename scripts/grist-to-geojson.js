const https = require('https');
const fs = require('fs');
const fetch = require('node-fetch');
const xml2js = require('xml2js');
const proj4 = require('proj4');

// Définition des projections
proj4.defs("EPSG:2154", "+proj=lcc +lat_1=49 +lat_2=44 +lat_0=46.5 +lon_0=3 +x_0=700000 +y_0=6600000 +ellps=GRS80 +units=m +no_defs");
proj4.defs("EPSG:3948", "+proj=lcc +lat_0=48 +lon_0=3 +lat_1=47.25 +lat_2=48.75 +x_0=1700000 +y_0=7200000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs +type=crs");

const GRIST_DOC_ID = process.env.GRIST_DOC_ID;
const GRIST_API_KEY = process.env.GRIST_API_KEY;
const TABLE_ID = 'Signalements';

// ✨ DIRO : Chemin vers le fichier GeoJSON généré par le script Python
const DIRO_FILE_PATH = 'data/inondations-diro.geojson';

// Compteur global pour générer des IDs uniques
let uniqueIdCounter = 1;

function generateUniqueId() {
    return uniqueIdCounter++;
}

// Fonction pour obtenir la date/heure en format français (timezone Europe/Paris)
function getDateTimeFR() {
    const now = new Date();
    
    // Obtenir la date/heure en timezone française
    const optionsDate = { 
        timeZone: 'Europe/Paris',
        day: '2-digit',
        month: '2-digit', 
        year: 'numeric'
    };
    const optionsTime = { 
        timeZone: 'Europe/Paris',
        hour: '2-digit', 
        minute: '2-digit',
        hour12: false
    };
    
    const dateFR = now.toLocaleDateString('fr-FR', optionsDate);
    const timeFR = now.toLocaleTimeString('fr-FR', optionsTime);
    
    return {
        iso: now.toISOString(),  // Format ISO UTC standard
        local: `${dateFR} à ${timeFR}`,  // Format français lisible
        timezone: 'Europe/Paris'
    };
}

console.log('🚀 Démarrage de la fusion des 7 sources...\n');
console.log('   1. Grist 35 (signalements manuels)');
console.log('   2. CD44 (API REST)');
console.log('   3. Rennes Métropole (WFS routes coupées)');
console.log('   4. CD35 Inondations (WFS XML)');
console.log('   5. CD56 (OGC API REST)');
console.log('   6. ✨ DIRO - DIR Ouest (DATEX II flash floods)\n');

// =====================================================
// CONFIGURATION
// =====================================================

const CD35_WFS_CONFIG = {
    url: 'https://dservices1.arcgis.com/jGLANYlFVVx3nuxa/arcgis/services/Inondations_cd35/WFSServer',
    typeName: 'Inondations_cd35:Inondation',
    srsName: 'EPSG:2154'
};

const CD56_OGC_BASE = 'https://services.arcgis.com/4GFMPbPboxIs6KOG/arcgis/rest/services/INONDATION/OGCFeatureServer';

const RENNES_METRO_WFS_URL = 'https://public.sig.rennesmetropole.fr/geoserver/ows?SERVICE=WFS&REQUEST=GetFeature&VERSION=2.0.0&TYPENAMES=trp_rout:routes_coupees&OUTPUTFORMAT=json';

// ✅ FONCTION POUR VÉRIFIER SI UNE DATE EST SUPÉRIEURE À 3 JOURS
function isOlderThan3Days(dateString) {
    if (!dateString) return false;
    
    try {
        // Parser le format "DD/MM/YYYY à HHhMM"
        const match = dateString.match(/(\d{2})\/(\d{2})\/(\d{4})\s+à\s+(\d{2})h(\d{2})/);
        if (!match) return false;
        
        const [_, day, month, year, hours, minutes] = match;
        
        // Créer un objet Date en heure locale française
        const dateObj = new Date(year, month - 1, day, hours, minutes);
        
        // Vérifier validité
        if (isNaN(dateObj.getTime())) return false;
        
        // Calculer la différence en millisecondes
        const now = new Date();
        const diffMs = now - dateObj;
        
        // Convertir en jours (1 jour = 86400000 ms)
        const diffDays = diffMs / (1000 * 60 * 60 * 24);
        
        // Retourner true si > 3 jours
        return diffDays > 3;
        
    } catch (e) {
        return false;
    }
}

// ✅ FONCTION POUR FILTRER LES SIGNALEMENTS RÉSOLUS DEPUIS PLUS DE 3 JOURS
function shouldKeepFeature(feature) {
    const props = feature.properties;
    
    // Si le signalement est actif, on le garde toujours
    if (props.statut_actif === true) {
        return { keep: true, filteredResolved: false };
    }
    
    // Si le signalement est résolu
    if (props.statut_resolu === true) {
        // Vérifier la date de fin
        if (props.date_fin && isOlderThan3Days(props.date_fin)) {
            // Résolu depuis plus de 3 jours → on le filtre
            return { keep: false, filteredResolved: true };
        }
        // Si pas de date_fin ou < 3 jours, on le garde
        return { keep: true, filteredResolved: false };
    }
    
    // Par défaut, on garde
    return { keep: true, filteredResolved: false };
}

// =====================================================
// SYSTÈME D'ARCHIVAGE ANNUEL
// =====================================================

// Charger un fichier d'archive (ou créer vide)
function loadArchive(year) {
    const archiveDir = 'archives';
    const archivePath = `${archiveDir}/signalements_${year}.geojson`;
    
    // Créer le dossier archives s'il n'existe pas
    if (!fs.existsSync(archiveDir)) {
        fs.mkdirSync(archiveDir, { recursive: true });
    }
    
    // Si le fichier existe, le charger
    if (fs.existsSync(archivePath)) {
        try {
            const content = fs.readFileSync(archivePath, 'utf8');
            return JSON.parse(content);
        } catch (e) {
            console.warn(`⚠️ Erreur lecture archive ${year}, création nouvelle:`, e.message);
            return {
                type: 'FeatureCollection',
                features: [],
                metadata: {
                    year: year,
                    created: new Date().toISOString(),
                    last_update: new Date().toISOString()
                }
            };
        }
    }
    
    // Sinon créer un nouveau GeoJSON vide
    return {
        type: 'FeatureCollection',
        features: [],
        metadata: {
            year: year,
            created: new Date().toISOString(),
            last_update: new Date().toISOString()
        }
    };
}

// Sauvegarder une archive
function saveArchive(year, geojson) {
    const archiveDir = 'archives';
    const archivePath = `${archiveDir}/signalements_${year}.geojson`;
    
    geojson.metadata.last_update = new Date().toISOString();
    
    fs.writeFileSync(archivePath, JSON.stringify(geojson, null, 2));
}

// Charger last_run.json
function loadLastRun() {
    const lastRunPath = 'archives/last_run.json';
    
    if (fs.existsSync(lastRunPath)) {
        try {
            const content = fs.readFileSync(lastRunPath, 'utf8');
            return JSON.parse(content);
        } catch (e) {
            console.warn('⚠️ Erreur lecture last_run.json:', e.message);
            return { date: null, actifs: {} };
        }
    }
    
    return { date: null, actifs: {} };
}

// Sauvegarder last_run.json
function saveLastRun(data) {
    const lastRunPath = 'archives/last_run.json';
    fs.writeFileSync(lastRunPath, JSON.stringify(data, null, 2));
}

// Extraire l'année de date_debut
function getYearFromDateDebut(dateString) {
    if (!dateString) return null;
    
    try {
        // Format: "DD/MM/YYYY à HHhMM"
        const match = dateString.match(/\d{2}\/\d{2}\/(\d{4})/);
        if (match) {
            return parseInt(match[1]);
        }
        return null;
    } catch (e) {
        return null;
    }
}

// Trouver un signalement dans l'archive par id_source + source
function findInArchive(archive, idSource, source) {
    if (!archive || !archive.features) return -1;
    
    return archive.features.findIndex(f => 
        f.properties.id_source === idSource && 
        f.properties.source === source
    );
}

// Ajouter ou mettre à jour un signalement dans l'archive
function addOrUpdateInArchive(feature) {
    const props = feature.properties;
    
    // Ignorer CD35 Inondations et DIRO
    if (props.source === 'CD35 Inondations' || props.source === 'DIRO') {
        return;
    }
    
    // Extraire l'année
    const year = getYearFromDateDebut(props.date_debut);
    if (!year) {
        console.warn(`⚠️ Pas d'année pour ${props.source} - ${props.id_source}`);
        return;
    }
    
    // Charger l'archive de cette année
    const archive = loadArchive(year);
    
    // Chercher si le signalement existe déjà
    const existingIndex = findInArchive(archive, props.id_source, props.source);
    
    if (existingIndex >= 0) {
        // Un signalement avec le même id_source existe
        const existing = archive.features[existingIndex];
        const existingProps = existing.properties;
        
        // ✨ VÉRIFICATION : Comparer les dates_debut pour détecter les ID réutilisés
        if (existingProps.date_debut !== props.date_debut) {
            // C'est un NOUVEAU signalement différent avec le même ID réutilisé !
            // Ne pas mettre à jour, créer une nouvelle entrée
            const archiveFeature = {
                ...feature,
                properties: {
                    ...props,
                    date_suppression: ''
                }
            };
            archive.features.push(archiveFeature);
            console.log(`   ➕ Nouvel signalement ${props.source} ${props.id_source} (ID réutilisé) dans archive ${year}`);
        } else {
            // Même date_debut = vraiment le même signalement, mise à jour possible
            
            // Si le statut a changé vers "Résolu", ajouter date_fin
            if (!existingProps.statut_resolu && props.statut_resolu && props.date_fin) {
                existingProps.statut = 'Résolu';
                existingProps.statut_resolu = true;
                existingProps.date_fin = props.date_fin;
                console.log(`   ✏️ Mise à jour ${props.source} ${props.id_source}: Actif → Résolu`);
            }
            
            // Mettre à jour la géométrie et autres infos (au cas où)
            existing.geometry = feature.geometry;
            existingProps.type_coupure = props.type_coupure;
            existingProps.commentaire = props.commentaire;
        }
        
    } else {
        // Nouveau signalement, l'ajouter
        const archiveFeature = {
            ...feature,
            properties: {
                ...props,
                date_suppression: ''
            }
        };
        archive.features.push(archiveFeature);
        console.log(`   ➕ Ajout ${props.source} ${props.id_source} dans archive ${year}`);
    }
    
    // Sauvegarder l'archive
    saveArchive(year, archive);
}

// Détecter et marquer les signalements supprimés
function detectDeletedSignalements(currentFeatures) {
    const lastRun = loadLastRun();
    const now = new Date();
    const dateSuppressionFormatted = `${String(now.getDate()).padStart(2, '0')}/${String(now.getMonth() + 1).padStart(2, '0')}/${now.getFullYear()} à ${String(now.getHours()).padStart(2, '0')}h${String(now.getMinutes()).padStart(2, '0')}`;
    
    // Construire la liste des id_source actuels par source
    const currentActifs = {
        'Saisie Grist': [],
        'CD44': [],
        'Rennes Métropole': [],
        'CD56': []
    };
    
    currentFeatures.forEach(feature => {
        const props = feature.properties;
        
        // Ignorer CD35 et DIRO
        if (props.source === 'CD35 Inondations' || props.source === 'DIRO') return;
        
        // Seulement les actifs
        if (props.statut_actif && props.id_source) {
            if (currentActifs[props.source]) {
                currentActifs[props.source].push(props.id_source);
            }
        }
    });
    
    // Si c'est la première exécution, juste sauvegarder
    if (!lastRun.date) {
        console.log('   ℹ️ Première exécution - initialisation de last_run.json');
        saveLastRun({
            date: now.toISOString(),
            actifs: currentActifs
        });
        return;
    }
    
    // Comparer avec la dernière exécution
    let deletedCount = 0;
    
    Object.keys(lastRun.actifs || {}).forEach(source => {
        const previousIds = lastRun.actifs[source] || [];
        const currentIds = currentActifs[source] || [];
        
        previousIds.forEach(idSource => {
            // Si l'ID n'est plus dans les actifs actuels
            if (!currentIds.includes(idSource)) {
                // Chercher dans quelle archive il est
                // On doit parcourir plusieurs années potentielles
                const currentYear = now.getFullYear();
                const yearsToCheck = [currentYear, currentYear - 1]; // Année actuelle + année précédente
                
                let found = false;
                yearsToCheck.forEach(year => {
                    if (found) return;
                    
                    const archive = loadArchive(year);
                    const index = findInArchive(archive, idSource, source);
                    
                    if (index >= 0) {
                        const feature = archive.features[index];
                        
                        // Seulement marquer comme supprimé s'il était actif
                        if (feature.properties.statut_actif && !feature.properties.date_suppression) {
                            feature.properties.statut = 'Supprimé';
                            feature.properties.statut_actif = false;
                            feature.properties.date_suppression = dateSuppressionFormatted;
                            
                            saveArchive(year, archive);
                            console.log(`   🗑️ Suppression détectée: ${source} ${idSource} (archive ${year})`);
                            deletedCount++;
                            found = true;
                        }
                    }
                });
            }
        });
    });
    
    if (deletedCount > 0) {
        console.log(`   📊 Total suppressions détectées: ${deletedCount}`);
    }
    
    // Sauvegarder le nouvel état
    saveLastRun({
        date: now.toISOString(),
        actifs: currentActifs
    });
}

// ✅ FONCTION DE FORMATAGE DES DATES - Convertit UTC → Heure locale française
function formatDate(dateValue) {
    if (!dateValue) return '';
    
    try {
        let date;
        
        // Si c'est une string ISO
        if (typeof dateValue === 'string') {
            date = new Date(dateValue);
        } 
        // Si c'est un timestamp
        else if (typeof dateValue === 'number') {
            // ArcGIS retourne des timestamps en millisecondes (> 1000000000000)
            // Sinon c'est en secondes
            if (dateValue > 100000000000) {
                date = new Date(dateValue); // Déjà en millisecondes
            } else {
                date = new Date(dateValue * 1000); // En secondes, convertir en millisecondes
            }
        } else {
            return '';
        }
        
        // Vérifier validité
        if (isNaN(date.getTime())) {
            return '';
        }
        
        // Conversion vers heure locale française (Europe/Paris)
        // toLocaleString avec timeZone Europe/Paris garantit la bonne conversion
        const options = {
            timeZone: 'Europe/Paris',
            day: '2-digit',
            month: '2-digit', 
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            hour12: false
        };
        
        const formatted = date.toLocaleString('fr-FR', options);
        // Format retourné: "17/11/2025 15:39" ou "17/11/2025, 15:39"
        
        // Parser le résultat pour obtenir notre format
        const match = formatted.match(/(\d{2})\/(\d{2})\/(\d{4})[,\s]+(\d{2}):(\d{2})/);
        if (match) {
            const [_, day, month, year, hours, minutes] = match;
            return `${day}/${month}/${year} à ${hours}h${minutes}`;
        }
        
        return '';
        
    } catch (e) {
        return '';
    }
}

// =====================================================
// CONVERSION LAMBERT 93 → WGS84
// =====================================================

function convertLambert93ToWGS84(x, y) {
    return proj4("EPSG:2154", "EPSG:4326", [x, y]);
}

function convertCC48ToWGS84(x, y) {
    return proj4("EPSG:3948", "EPSG:4326", [x, y]);
}

// =====================================================
// RENNES MÉTROPOLE - WFS ROUTES COUPÉES
// =====================================================

async function fetchRennesMetroData() {
    try {
        console.log(`🔗 [Rennes Métropole] Récupération via WFS...`);
        
        const response = await fetch(RENNES_METRO_WFS_URL, {
            headers: {
                'User-Agent': 'Mozilla/5.0'
            }
        });
        
        if (!response.ok) {
            console.error(`❌ [Rennes Métropole] HTTP ${response.status}`);
            return [];
        }
        
        const geojson = await response.json();
        console.log(`   GeoJSON reçu: ${geojson.features?.length || 0} features`);
        
        // Filtrer uniquement raison="inondation"
        const filteredFeatures = (geojson.features || []).filter(feature => {
            const raison = feature.properties?.raison || '';
            return raison.toLowerCase() === 'inondation';
        });
        
        console.log(`   Filtrés (raison=inondation): ${filteredFeatures.length} features`);
        
        // Vérifier la projection en examinant les coordonnées
        // CC48 (EPSG:3948): X entre 1300000-2100000, Y entre 7000000-7500000
        // WGS84: longitude entre -180 et 180, latitude entre -90 et 90
        let needsConversion = false;
        if (filteredFeatures.length > 0) {
            const firstGeom = filteredFeatures[0].geometry;
            if (firstGeom) {
                let testCoord;
                // Extraire le premier X selon le type de géométrie
                if (firstGeom.type === 'Point') {
                    testCoord = firstGeom.coordinates[0];
                } else if (firstGeom.type === 'LineString') {
                    // Pour LineString: coordinates = [[x1,y1], [x2,y2], ...]
                    testCoord = firstGeom.coordinates[0][0];
                } else if (firstGeom.type === 'MultiLineString') {
                    // Pour MultiLineString: coordinates = [[[x1,y1], [x2,y2]], [[x3,y3], ...]]
                    testCoord = firstGeom.coordinates[0][0][0];
                }
                
                if (testCoord && Math.abs(testCoord) > 1000) {
                    needsConversion = true;
                    console.log(`   ⚠️ Coordonnées détectées en projection CC48 (EPSG:3948): X=${testCoord}`);
                } else {
                    console.log(`   ✅ Coordonnées déjà en WGS84: X=${testCoord}`);
                }
            }
        }
        
        console.log(`✅ [Rennes Métropole] ${filteredFeatures.length} features filtrées avec succès`);
        return { features: filteredFeatures, needsConversion };
        
    } catch (error) {
        console.error(`❌ [Rennes Métropole]`, error.message);
        return { features: [], needsConversion: false };
    }
}

// Convertir Rennes Métropole
function rennesMetroToFeature(feature, needsConversion = false) {
    try {
        let geometry = feature.geometry;
        if (!geometry) return null;
        
        // Convertir la géométrie si nécessaire
        if (needsConversion) {
            if (geometry.type === 'Point') {
                const [x, y] = geometry.coordinates;
                const [lng, lat] = convertCC48ToWGS84(x, y);
                geometry = {
                    type: 'Point',
                    coordinates: [lng, lat]
                };
            } else if (geometry.type === 'LineString') {
                geometry = {
                    type: 'LineString',
                    coordinates: geometry.coordinates.map(([x, y]) => {
                        const [lng, lat] = convertCC48ToWGS84(x, y);
                        return [lng, lat];
                    })
                };
            } else if (geometry.type === 'MultiLineString') {
                geometry = {
                    type: 'MultiLineString',
                    coordinates: geometry.coordinates.map(line => 
                        line.map(([x, y]) => {
                            const [lng, lat] = convertCC48ToWGS84(x, y);
                            return [lng, lat];
                        })
                    )
                };
            }
        }
        
        const props = feature.properties || {};
        
        // Mapping des champs
        // comm_nom → commune
        // etat → statut ('terminé' = résolu, 'en cours' = actif)
        // date_debut → date de début et date de saisie
        // date_fin → date de fin (quand ça passe en terminé)
        // toponyme → nom de la route
        
        const etat = (props.etat || '').toLowerCase();
        const isResolu = etat === 'terminé' || etat === 'termine';
        const isActif = etat === 'en cours';
        
        const statut = isResolu ? 'Résolu' : (isActif ? 'Actif' : etat);
        
        // ID source : champ 'id' de Rennes Métropole
        const idSource = props.id || props.gid || null;
        
        return {
            type: 'Feature',
            geometry: geometry,
            properties: {
                id: generateUniqueId(),
                id_source: idSource,
                source: 'Rennes Métropole',
                route: props.toponyme || '',
                commune: props.comm_nom || '',
                cause: 'Inondation',
                statut: statut,
                statut_actif: isActif,
                statut_resolu: isResolu,
                type_coupure: 'Totale',
                sens_circulation: '',
                commentaire: props.commentaires || '',
                date_debut: formatDate(props.date_debut),
                date_fin: formatDate(props.date_fin),
                date_saisie: formatDate(props.date_debut), // date_debut comme date de saisie
                date_suppression: '',
                gestionnaire: 'Rennes Métropole'
            }
        };
    } catch (e) {
        console.error('Erreur conversion Rennes Métropole:', e.message);
        return null;
    }
}

// FONCTION CD35 AVEC RETRY - À remplacer dans votre script

async function fetchCD35InondationsData() {
    const maxRetries = 3;
    const retryDelay = 2000; // 2 secondes entre chaque tentative
    
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            console.log(`🔗 [CD35 Inondations] Tentative ${attempt}/${maxRetries}...`);
            
            const wfsUrl = `${CD35_WFS_CONFIG.url}?` +
                `service=WFS&` +
                `version=2.0.0&` +
                `request=GetFeature&` +
                `typeNames=${CD35_WFS_CONFIG.typeName}&` +
                `srsName=${CD35_WFS_CONFIG.srsName}`;
            
            const response = await fetch(wfsUrl, {
                headers: {
                    'User-Agent': 'Mozilla/5.0'
                },
                timeout: 10000 // 10 secondes de timeout
            });
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            
            const xmlText = await response.text();
            console.log(`   ✅ XML reçu (${xmlText.length} caractères)`);
            
            const parser = new xml2js.Parser({ 
                explicitArray: false,
                tagNameProcessors: [xml2js.processors.stripPrefix]
            });
            const json = await parser.parseStringPromise(xmlText);
            
            const features = [];
            let members = json.FeatureCollection?.member || [];
            if (!Array.isArray(members)) {
                members = [members];
            }
            
            console.log(`   ${members.length} members trouvés`);
            
            members.forEach(member => {
                try {
                    const inondation = member.Inondation;
                    if (!inondation) return;
                    
                    const shape = inondation.Shape || inondation.geometry;
                    if (!shape || !shape.Point || !shape.Point.pos) return;
                    
                    const coords = shape.Point.pos.split(' ');
                    const x = parseFloat(coords[0]);
                    const y = parseFloat(coords[1]);
                    if (isNaN(x) || isNaN(y)) return;
                    
                    const [lng, lat] = proj4("EPSG:2154", "EPSG:4326", [x, y]);
                    
                    features.push({
                        type: 'Feature',
                        geometry: { 
                            type: 'Point', 
                            coordinates: [lng, lat] 
                        },
                        properties: {
                            OBJECTID: inondation.OBJECTID,
                            route: inondation.route,
                            etat_circulation: inondation.etat_circulation,
                            commune: inondation.commune,
                            agence: inondation.agence,
                            PR_debut: inondation.PR_début,
                            PR_fin: inondation.PR_fin,
                            lieu_dit: inondation.lieu_dit
                        }
                    });
                    
                } catch (e) {
                    console.warn(`   ⚠️ Erreur parsing feature:`, e.message);
                }
            });
            
            console.log(`✅ [CD35 Inondations] ${features.length} features parsées`);
            return features;
            
        } catch (error) {
            console.error(`❌ [CD35 Inondations] Tentative ${attempt} échouée:`, error.message);
            
            if (attempt < maxRetries) {
                console.log(`   ⏳ Attente de ${retryDelay/1000}s avant nouvelle tentative...`);
                await new Promise(resolve => setTimeout(resolve, retryDelay));
            } else {
                console.error(`❌ [CD35 Inondations] Échec après ${maxRetries} tentatives`);
                return [];
            }
        }
    }
    
    return [];
}

// Récupérer Grist
async function fetchGristData() {
    try {
        if (!GRIST_DOC_ID || !GRIST_API_KEY) {
            console.warn('⚠️ Grist credentials manquants');
            return [];
        }

        console.log('🔗 [Grist 35] Récupération...');
        
        const options = {
            hostname: 'grist.dataregion.fr',
            path: `/o/inforoute/api/docs/${GRIST_DOC_ID}/tables/${TABLE_ID}/records`,
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${GRIST_API_KEY}`,
                'Content-Type': 'application/json'
            }
        };

        return new Promise((resolve) => {
            https.get(options, (res) => {
                let data = '';
                res.on('data', chunk => { data += chunk; });
                res.on('end', () => {
                    if (res.statusCode === 200) {
                        try {
                            const parsed = JSON.parse(data);
                            console.log(`✅ [Grist 35] ${parsed.records.length} records`);
                            resolve(parsed.records || []);
                        } catch (e) {
                            console.error('❌ [Grist 35] Parse error');
                            resolve([]);
                        }
                    } else {
                        console.error(`❌ [Grist 35] HTTP ${res.statusCode}`);
                        resolve([]);
                    }
                });
            }).on('error', (err) => {
                console.error('❌ [Grist 35]', err.message);
                resolve([]);
            });
        });
    } catch (error) {
        console.error('❌ [Grist 35]', error.message);
        return [];
    }
}

// Récupérer CD44
async function fetchCD44Data() {
    try {
        console.log('🔗 [CD44] Récupération...');
        
        return new Promise((resolve) => {
            const options = {
                hostname: 'data.loire-atlantique.fr',
                path: '/api/explore/v2.1/catalog/datasets/224400028_info-route-departementale/records?limit=100',
                method: 'GET',
                headers: {
                    'User-Agent': 'Mozilla/5.0'
                }
            };

            https.get(options, (res) => {
                let data = '';
                res.on('data', chunk => { data += chunk; });
                res.on('end', () => {
                    if (res.statusCode === 200) {
                        try {
                            const response = JSON.parse(data);
                            const records = response.results || [];
                            console.log(`✅ [CD44] ${records.length} records`);
                            resolve(records);
                        } catch (e) {
                            console.error('❌ [CD44] Parse error');
                            resolve([]);
                        }
                    } else {
                        console.error(`❌ [CD44] HTTP ${res.statusCode}`);
                        resolve([]);
                    }
                });
            }).on('error', (err) => {
                console.error('❌ [CD44]', err.message);
                resolve([]);
            });
        });
    } catch (error) {
        console.error('❌ [CD44]', error.message);
        return [];
    }
}



// Récupérer CD56 (OGC API REST)
async function fetchCD56Data() {
    try {
        console.log(`🔗 [CD56] Récupération via OGC API REST...`);
        
        // D'abord, récupérer la liste des collections pour trouver le bon ID
        const collectionsUrl = `${CD56_OGC_BASE}/collections?f=json`;
        console.log(`   URL collections: ${collectionsUrl.substring(0, 80)}...`);
        
        const collectionsResponse = await fetch(collectionsUrl, {
            headers: {
                'User-Agent': 'Mozilla/5.0'
            }
        });
        
        if (!collectionsResponse.ok) {
            console.error(`❌ [CD56] HTTP ${collectionsResponse.status} sur /collections`);
            return [];
        }
        
        const collectionsData = await collectionsResponse.json();
        
        // Trouver la première collection (ou celle qui contient "Inondation")
        const collections = collectionsData.collections || [];
        if (collections.length === 0) {
            console.error(`❌ [CD56] Aucune collection trouvée`);
            return [];
        }
        
        const collection = collections[0]; // Prendre la première
        const collectionId = collection.id;
        console.log(`   Collection trouvée: ${collectionId}`);
        
        // Maintenant récupérer les items
        const itemsUrl = `${CD56_OGC_BASE}/collections/${collectionId}/items?f=json`;
        console.log(`   URL items: ${itemsUrl.substring(0, 80)}...`);
        
        const itemsResponse = await fetch(itemsUrl, {
            headers: {
                'User-Agent': 'Mozilla/5.0'
            }
        });
        
        if (!itemsResponse.ok) {
            console.error(`❌ [CD56] HTTP ${itemsResponse.status} sur /items`);
            return [];
        }
        
        const data = await itemsResponse.json();
        console.log(`   Réponse JSON reçue`);
        
        // L'API OGC retourne les features dans data.features
        const features = data.features || [];
        
        // Logger les propriétés de la première feature pour debug
        if (features.length > 0) {
            console.log(`   🔍 Exemple de propriétés CD56 (première feature):`);
            console.log(JSON.stringify(features[0].properties, null, 2));
        }
        
        console.log(`✅ [CD56] ${features.length} features récupérées avec succès`);
        
        return features;
        
    } catch (error) {
        console.error(`❌ [CD56]`, error.message);
        return [];
    }
}

// Convertir Grist
function gristToFeature(record) {
    try {
        let geometry;
        
        if (record.fields.geojson) {
            geometry = JSON.parse(record.fields.geojson);
        } else if (record.fields.Latitude && record.fields.Longitude) {
            geometry = {
                type: 'Point',
                coordinates: [record.fields.Longitude, record.fields.Latitude]
            };
        } else {
            return null;
        }
        
        const cause = Array.isArray(record.fields.Cause) ? 
                     record.fields.Cause.filter(c => c !== 'L').join(', ') : 
                     (record.fields.Cause || '');
        
        const statut = record.fields.Statut || 'Actif';
        
        // ID source : champ 'id' de Grist
        const idSource = record.id || null;
        
        return {
            type: 'Feature',
            geometry: geometry,
            properties: {
                id: generateUniqueId(),
                id_source: idSource,
                source: 'Saisie Grist',
                route: record.fields.Route || '',
                commune: record.fields.Commune || '',
                cause: cause || 'Inondation',
                statut: statut,
                statut_actif: statut === 'Actif',
                statut_resolu: statut === 'Résolu',
                type_coupure: record.fields.Type_coupure || 'Totale',
                sens_circulation: record.fields.sens_circulation || '',
                commentaire: record.fields.Description || '',
                date_debut: formatDate(record.fields.Date_heure),
                date_fin: formatDate(record.fields.Date_fin),
                date_saisie: formatDate(record.fields.Date_heure),
                date_suppression: '',
                gestionnaire: record.fields.Gestionnaire || ''
            }
        };
    } catch (e) {
        return null;
    }
}

// Fonction pour parser la date de fin depuis ligne4 (CD44)
function parseCD44DateFin(ligne4) {
    if (!ligne4) return '';
    
    try {
        // Format: "Du XX/XX/XXXX au DD/MM/AAAA"
        const duAuMatch = ligne4.match(/au\s+(\d{2})\/(\d{2})\/(\d{4})/);
        if (duAuMatch) {
            const [_, day, month, year] = duAuMatch;
            return `${day}/${month}/${year} à 00h00`;
        }
        
        // Format: "Fin prévisible : DD/MM/AAAA à HHhMM"
        const finMatch = ligne4.match(/(\d{2})\/(\d{2})\/(\d{4})\s+à\s+(\d{1,2})h(\d{2})/);
        if (finMatch) {
            const [_, day, month, year, hours, minutes] = finMatch;
            return `${day}/${month}/${year} à ${hours.padStart(2, '0')}h${minutes}`;
        }
        
        return ''; // Durée indéterminée ou format non reconnu
    } catch (e) {
        return '';
    }
}

// Convertir CD44
function cd44ToFeature(item) {
    try {
        // ✅ FILTRE : Ne garder que type="Obstacle"
     if (item.type !== 'Inondation' && item.type !== 'inondation') {
    return null;
}
        const geometry = {
            type: 'Point',
            coordinates: [item.longitude, item.latitude]
        };
        
        // ✅ Route depuis ligne2
        const route = Array.isArray(item.ligne2) ? item.ligne2.join(' / ') : (item.ligne2 || 'Route');
        
        // ✅ Commentaire = ligne1 + ligne5
        let commentaire = item.ligne1 || '';
        if (item.ligne5) {
            commentaire += (commentaire ? ' - ' : '') + item.ligne5;
        }
        
        // ✅ Date de fin extraite depuis ligne4
        const dateFin = parseCD44DateFin(item.ligne4);
        
        // ✅ Commune depuis ligne3 (ne pas mettre 'Commune' par défaut)
        const commune = item.ligne3 || '';
        
        const statut = 'Actif';
        
        // ID source : NULL pour CD44 (pas d'ID disponible)
        const idSource = null;
        
        return {
            type: 'Feature',
            geometry: geometry,
            properties: {
                id: generateUniqueId(),
                id_source: idSource,
                source: 'CD44',
                route: route,
                commune: commune,
                cause: 'Inondation',
                statut: statut,
                statut_actif: true,
                statut_resolu: false,
                type_coupure: 'Totale',
                sens_circulation: '',
                commentaire: commentaire,
                date_debut: formatDate(item.datepublication),
                date_fin: dateFin,
                date_saisie: formatDate(item.datepublication),
                date_suppression: '',
                gestionnaire: 'CD44'
            }
        };
    } catch (e) {
        return null;
    }
}


// Convertir CD35 Inondations
function cd35InondationsToFeature(feature) {
    try {
        const geometry = feature.geometry;
        if (!geometry) return null;
        
        const props = feature.properties || {};
        
        // ID source : pas d'ID distinct dans CD35, utiliser OBJECTID
        const idSource = props.OBJECTID || null;
        
        return {
            type: 'Feature',
            geometry: geometry,
            properties: {
                id: generateUniqueId(),
                id_source: idSource,
                source: 'CD35 Inondations',
                route: props.route || '',
                commune: props.commune || '',
                cause: 'Inondation',
                statut: 'Actif',
                statut_actif: true,
                statut_resolu: false,
                type_coupure: 'Totale',
                sens_circulation: '',
                commentaire: props.lieu_dit || '',
                date_debut: '',
                date_fin: '',
                date_saisie: new Date().toISOString(),
                date_suppression: '',
                gestionnaire: 'CD35',
                agence: props.agence || '',
                pr_debut: props.PR_debut || '',
                pr_fin: props.PR_fin || ''
            }
        };
    } catch (e) {
        console.error('Erreur conversion CD35 Inondations:', e.message);
        return null;
    }
}

// Convertir CD56
function cd56ToFeature(feature) {
    try {
        const geometry = feature.geometry;
        if (!geometry) return null;
        
        const props = feature.properties || {};
        
        // Filtre : ne garder que COUPÉE ou INONDÉE PARTIELLE
        const conditionsCirculation = props.conditions_circulation || props.conditionsCirculation || '';
        if (!['COUPÉE', 'INONDÉE PARTIELLE'].includes(conditionsCirculation.toUpperCase())) {
            return null;
        }
        
        // Déterminer le type de coupure
        const typeCoupure = conditionsCirculation.toUpperCase() === 'INONDÉE PARTIELLE' ? 'Partielle' : 'Totale';
        
        // Lineaire_inonde : seulement si différent de 0 et de "?"
        const lineaireInonde = props.lineaire_inonde || props.lineaireInonde || '';
        const lineaireInondeText = (lineaireInonde && lineaireInonde !== '0' && lineaireInonde !== '?') 
            ? `Longueur linéaire inondée : ${lineaireInonde}` 
            : '';
        
        // Commentaire : si INONDÉE PARTIELLE, on écrit "Inondation partielle" + lineaire_inonde
        let commentaire = '';
        if (conditionsCirculation.toUpperCase() === 'INONDÉE PARTIELLE') {
            commentaire = 'Inondation partielle';
            if (lineaireInondeText) {
                commentaire += `. ${lineaireInondeText}`;
            }
        } else if (lineaireInondeText) {
            commentaire = lineaireInondeText;
        }
        
        // ID source : champ 'OBJECTID' ou 'objectid' de CD56
        const idSource = props.OBJECTID || props.objectid || null;
        
        return {
            type: 'Feature',
            geometry: geometry,
            properties: {
                id: generateUniqueId(),
                id_source: idSource,
                source: 'CD56',
                route: props.rd || '',
                commune: props.commune || '',
                cause: 'Inondation',
                statut: 'Actif',
                statut_actif: true,
                statut_resolu: false,
                type_coupure: typeCoupure,
                sens_circulation: '',
                commentaire: commentaire,
                date_debut: formatDate(props.date_constatation || props.dateConstatation),
                date_fin: formatDate(props.Date_fin_d_évènement || props.date_fin_evenement || props.dateFin),
                date_saisie: formatDate(props.date_constatation || props.dateConstatation),
                date_suppression: '',
                gestionnaire: 'CD56'
            }
        };
    } catch (e) {
        console.error('Erreur conversion CD56:', e.message);
        return null;
    }
}

// =====================================================
// ✨ DIRO - DIR OUEST (DATEX II)
// =====================================================

/**
 * Récupère les données DIRO depuis le fichier GeoJSON généré par le script Python
 */
async function fetchDiroData() {
    try {
        console.log(`🔗 [DIRO] Lecture du fichier ${DIRO_FILE_PATH}...`);
        
        // Vérifier si le fichier existe
        if (!fs.existsSync(DIRO_FILE_PATH)) {
            console.log(`   ℹ️ Fichier DIRO non trouvé (${DIRO_FILE_PATH})`);
            return [];
        }
        
        // Lire le fichier
        const fileContent = fs.readFileSync(DIRO_FILE_PATH, 'utf8');
        const geojson = JSON.parse(fileContent);
        
        const features = geojson.features || [];
        console.log(`   ${features.length} features trouvées`);
        
        // Filtrer uniquement les inondations actives
        const activeFeatures = features.filter(f => f.properties.is_active === true);
        console.log(`   ${activeFeatures.length} inondations actives`);
        
        console.log(`✅ [DIRO] ${activeFeatures.length} inondations récupérées`);
        return activeFeatures;
        
    } catch (error) {
        console.error(`❌ [DIRO]`, error.message);
        return [];
    }
}

/**
 * Convertit une feature DIRO au format standard
 */
function diroToFeature(feature) {
    try {
        const geometry = feature.geometry;
        if (!geometry) return null;
        
        const props = feature.properties || {};
        
        // ID source : utiliser l'ID DATEX II
        const idSource = props.id || null;
        
        // Déterminer le statut
        const isActif = props.is_active === true;
        const isResolu = props.is_active === false;
        const statut = isActif ? 'Actif' : 'Résolu';
        
        // Mapper la sévérité vers un commentaire plus détaillé
        const severityText = {
            'low': 'Faible',
            'medium': 'Moyenne',
            'high': 'Élevée',
            'veryHigh': 'Très élevée'
        }[props.severity] || props.severity || '';
        
        const commentaire = [
            props.description || '',
            severityText ? `Sévérité: ${severityText}` : '',
            props.subtype ? `Type: ${props.subtype}` : ''
        ].filter(Boolean).join(' | ');
        
        return {
            type: 'Feature',
            geometry: geometry,
            properties: {
                id: generateUniqueId(),
                id_source: idSource,
                source: 'DIRO',
                route: props.road || '',
                commune: '', // DIRO ne fournit pas la commune
                cause: 'Inondation',
                statut: statut,
                statut_actif: isActif,
                statut_resolu: isResolu,
                type_coupure: 'Totale',
                sens_circulation: '',
                commentaire: commentaire,
                date_debut: formatDate(props.start_date),
                date_fin: formatDate(props.end_date),
                date_saisie: formatDate(props.start_date),
                date_suppression: '',
                gestionnaire: 'DIRO - DIR Ouest'
            }
        };
    } catch (e) {
        console.error('Erreur conversion DIRO:', e.message);
        return null;
    }
}

// Fusion principale
async function mergeSources() {
    try {
        console.log('');
        
        const [gristRecords, cd44Records, rennesMetroResult, cd35InondationsFeatures, cd56Features] = await Promise.all([
            fetchGristData(),
            fetchCD44Data(),
            fetchRennesMetroData(),
            fetchCD35InondationsData(),
            fetchCD56Data()
        ]);
        
        // ✨ Récupérer les données DIRO (lecture fichier local)
        const diroFeatures = await fetchDiroData();
        
        const rennesMetroFeatures = rennesMetroResult.features || [];
        const needsConversion = rennesMetroResult.needsConversion || false;
        
        const totalBrut = gristRecords.length + cd44Records.length + rennesMetroFeatures.length +
                         cd35InondationsFeatures.length + cd56Features.length + diroFeatures.length;
        console.log(`\n📊 Total brut récupéré: ${totalBrut} records\n`);
        
        let features = [];
        let stats = {
            grist_recupere: gristRecords.length,
            grist_garde: 0,
            cd44_recupere: cd44Records.length,
            cd44_garde: 0,
            rennes_recupere: rennesMetroFeatures.length,
            rennes_garde: 0,
            cd35_recupere: cd35InondationsFeatures.length,
            cd35_garde: 0,
            cd56_recupere: cd56Features.length,
            cd56_garde: 0,
            diro_recupere: diroFeatures.length,
            diro_garde: 0,
            resolus_filtres: 0  // Compteur pour les résolus > 3 jours
        };
        
        // Grist 35
        gristRecords.forEach(record => {
            const feature = gristToFeature(record);
            if (feature) {
                const result = shouldKeepFeature(feature);
                if (result.keep) {
                    features.push(feature);
                    stats.grist_garde++;
                } else if (result.filteredResolved) {
                    stats.resolus_filtres++;
                }
            }
        });
        console.log(`   Grist 35: ${stats.grist_recupere} récupérés → ${stats.grist_garde} gardés`);
        
        // CD44
        cd44Records.forEach(item => {
            const feature = cd44ToFeature(item);
            if (feature) {
                const result = shouldKeepFeature(feature);
                if (result.keep) {
                    features.push(feature);
                    stats.cd44_garde++;
                } else if (result.filteredResolved) {
                    stats.resolus_filtres++;
                }
            }
        });
        console.log(`   CD44: ${stats.cd44_recupere} récupérés → ${stats.cd44_garde} gardés`);
        
        // Rennes Métropole
        rennesMetroFeatures.forEach(feature => {
            const converted = rennesMetroToFeature(feature, needsConversion);
            if (converted) {
                const result = shouldKeepFeature(converted);
                if (result.keep) {
                    features.push(converted);
                    stats.rennes_garde++;
                } else if (result.filteredResolved) {
                    stats.resolus_filtres++;
                }
            }
        });
        console.log(`   Rennes Métropole: ${stats.rennes_recupere} récupérés → ${stats.rennes_garde} gardés`);
        
        // CD35 Inondations
        cd35InondationsFeatures.forEach(feature => {
            const converted = cd35InondationsToFeature(feature);
            if (converted) {
                const result = shouldKeepFeature(converted);
                if (result.keep) {
                    features.push(converted);
                    stats.cd35_garde++;
                } else if (result.filteredResolved) {
                    stats.resolus_filtres++;
                }
            }
        });
        console.log(`   CD35: ${stats.cd35_recupere} récupérés → ${stats.cd35_garde} gardés`);
        
        // CD56
        cd56Features.forEach(feature => {
            const converted = cd56ToFeature(feature);
            if (converted) {
                const result = shouldKeepFeature(converted);
                if (result.keep) {
                    features.push(converted);
                    stats.cd56_garde++;
                } else if (result.filteredResolved) {
                    stats.resolus_filtres++;
                }
            }
        });
        console.log(`   CD56: ${stats.cd56_recupere} récupérés → ${stats.cd56_garde} gardés`);
        
        // ✨ DIRO
        diroFeatures.forEach(feature => {
            const converted = diroToFeature(feature);
            if (converted) {
                const result = shouldKeepFeature(converted);
                if (result.keep) {
                    features.push(converted);
                    stats.diro_garde++;
                } else if (result.filteredResolved) {
                    stats.resolus_filtres++;
                }
            }
        });
        console.log(`   DIRO: ${stats.diro_recupere} récupérés → ${stats.diro_garde} gardés`);
        
        const totalGarde = stats.grist_garde + stats.cd44_garde + stats.rennes_garde + stats.cd35_garde + stats.cd56_garde + stats.diro_garde;
        const totalFiltre = totalBrut - totalGarde;
        
        // =====================================================
        // ARCHIVAGE ANNUEL
        // =====================================================
        console.log(`\n📦 Archivage annuel...`);
        
        // Archiver tous les signalements (sauf CD35)
        features.forEach(feature => {
            addOrUpdateInArchive(feature);
        });
        
        // Détecter les signalements supprimés
        console.log(`\n🔍 Détection des suppressions...`);
        detectDeletedSignalements(features);
        
        console.log(`✅ Archivage terminé\n`);
        
        // =====================================================
        // FIN ARCHIVAGE
        // =====================================================
        
        console.log(`\n📊 Résumé:`);
        console.log(`   Total récupéré: ${totalBrut}`);
        console.log(`   Total gardé: ${totalGarde}`);
        console.log(`   Total filtré: ${totalFiltre}`);
        console.log(`   → dont résolus > 3 jours: ${stats.resolus_filtres}\n`);
        
        const geojson = {
            type: 'FeatureCollection',
            features: features,
            metadata: {
                generated: new Date().toISOString(),
                source: 'Fusion Grist 35 + CD44 + Rennes Métropole + CD35 Inondations + CD56 + DIRO',
                total_count: features.length,
                sources: {
                    grist_35: gristRecords.length,
                    cd44: cd44Records.length,
                    rennes_metropole: rennesMetroFeatures.length,
                    cd35_inondations: cd35InondationsFeatures.length,
                    cd56: cd56Features.length,
                    diro: diroFeatures.length
                }
            }
        };
        
        fs.writeFileSync('signalements.geojson', JSON.stringify(geojson, null, 2));
        console.log('✅ Fichier signalements.geojson créé');
        
        // Obtenir la date/heure française
        const dateTimeFR = getDateTimeFR();
        
        // Calculer les statistiques par administration
        const administrations = {};
        features.forEach(feature => {
            const admin = feature.properties.administration || 'Non spécifié';
            administrations[admin] = (administrations[admin] || 0) + 1;
        });
        
        // Calculer les statistiques par source
        const parSource = {
            saisie_grist: features.filter(f => f.properties.source === 'Saisie Grist').length,
            cd44: features.filter(f => f.properties.source === 'CD44').length,
            rennes_metropole: features.filter(f => f.properties.source === 'Rennes Métropole').length,
            cd35_inondations: features.filter(f => f.properties.source === 'CD35 Inondations').length,
            cd56: features.filter(f => f.properties.source === 'CD56').length,
            diro: features.filter(f => f.properties.source === 'DIRO').length
        };
        
        const metadata = {
            // ⏰ Informations temporelles
            lastUpdate: dateTimeFR.iso,           // Format ISO UTC (standard)
            lastUpdateFR: dateTimeFR.local,       // Format français lisible
            timezone: dateTimeFR.timezone,
            nextUpdateIn: '30 minutes',
            
            // 📊 Comptages globaux
            totalRecus: totalBrut,
            totalInclus: totalGarde,
            totalFiltres: totalFiltre,
            resolus_filtres_3jours: stats.resolus_filtres,
            
            // 📡 Données brutes récupérées par source
            sources_recues: {
                grist_35: gristRecords.length,
                cd44: cd44Records.length,
                rennes_metropole: rennesMetroFeatures.length,
                cd35_inondations: cd35InondationsFeatures.length,
                cd56: cd56Features.length,
                diro: diroFeatures.length
            },
            
            // ✅ Données incluses par source (après filtrage)
            sources_incluses: parSource,
            
            // 🗺️ Par type de géométrie
            geometries: {
                points: features.filter(f => f.geometry.type === 'Point').length,
                lignes: features.filter(f => f.geometry.type === 'LineString').length,
                multilignes: features.filter(f => f.geometry.type === 'MultiLineString').length,
                polygones: features.filter(f => f.geometry.type === 'Polygon').length
            },
            
            // 🏛️ Par administration/gestionnaire
            administrations: administrations,
            
            // 📦 Informations sur l'archivage
            archives: {
                enabled: true,
                location: 'archives/',
                description: 'Historique annuel permanent (sauf CD35 Inondations et DIRO)',
                note: 'Les signalements sont archivés par année (date_debut) et suivis pour détecter les suppressions'
            }
        };
        
        fs.writeFileSync('metadata.json', JSON.stringify(metadata, null, 2));
        console.log('✅ Métadonnées créées');
        
        console.log('\n📊 Statistiques finales:');
        console.log(`   - Heure mise à jour: ${dateTimeFR.local}`);
        console.log(`   - Total reçu: ${totalBrut}`);
        console.log(`   - Total inclus: ${totalGarde}`);
        console.log(`   - Total filtré: ${totalFiltre}`);
        console.log(`   - Grist 35: ${gristRecords.length}`);
        console.log(`   - CD44: ${cd44Records.length}`);
        console.log(`   - Rennes Métropole: ${rennesMetroFeatures.length}`);
        console.log(`   - CD35 Inondations: ${cd35InondationsFeatures.length}`);
        console.log(`   - CD56: ${cd56Features.length}`);
        console.log(`   - ✨ DIRO: ${diroFeatures.length}`);
        console.log(`   - Points: ${metadata.geometries.points}`);
        console.log(`   - LineStrings: ${metadata.geometries.lignes}`);
        console.log(`   - Polygons: ${metadata.geometries.polygones}`);
        console.log('\n🏛️ Par administration:');
        Object.entries(administrations).forEach(([admin, count]) => {
            console.log(`   - ${admin}: ${count}`);
        });
        
    } catch (error) {
        console.error('❌ Erreur fusion:', error.message);
        process.exit(1);
    }
}

mergeSources();
