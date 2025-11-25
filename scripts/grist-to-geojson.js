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

// Compteur global pour générer des IDs uniques
let uniqueIdCounter = 1;

function generateUniqueId() {
    return uniqueIdCounter++;
}

console.log('🚀 Démarrage de la fusion des 6 sources...\n');
console.log('   1. Grist 35 (signalements manuels)');
console.log('   2. CD44 (API REST)');
console.log('   3. Rennes Métropole (WFS routes coupées)');
console.log('   4. CD35 Inondations (WFS XML)');
console.log('   5. CD56 (OGC API REST)\n');

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
        if (item.type !== 'Obstacle') {
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
                gestionnaire: 'CD56'
            }
        };
    } catch (e) {
        console.error('Erreur conversion CD56:', e.message);
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
        
        const rennesMetroFeatures = rennesMetroResult.features || [];
        const needsConversion = rennesMetroResult.needsConversion || false;
        
        const totalBrut = gristRecords.length + cd44Records.length + rennesMetroFeatures.length +
                         cd35InondationsFeatures.length + cd56Features.length;
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
            cd56_garde: 0
        };
        
        // Grist 35
        gristRecords.forEach(record => {
            const feature = gristToFeature(record);
            if (feature) {
                features.push(feature);
                stats.grist_garde++;
            }
        });
        console.log(`   Grist 35: ${stats.grist_recupere} récupérés → ${stats.grist_garde} gardés`);
        
        // CD44
        cd44Records.forEach(item => {
            const feature = cd44ToFeature(item);
            if (feature) {
                features.push(feature);
                stats.cd44_garde++;
            }
        });
        console.log(`   CD44: ${stats.cd44_recupere} récupérés → ${stats.cd44_garde} gardés`);
        
        // Rennes Métropole
        rennesMetroFeatures.forEach(feature => {
            const converted = rennesMetroToFeature(feature, needsConversion);
            if (converted) {
                features.push(converted);
                stats.rennes_garde++;
            }
        });
        console.log(`   Rennes Métropole: ${stats.rennes_recupere} récupérés → ${stats.rennes_garde} gardés`);
        
        // CD35 Inondations
        cd35InondationsFeatures.forEach(feature => {
            const converted = cd35InondationsToFeature(feature);
            if (converted) {
                features.push(converted);
                stats.cd35_garde++;
            }
        });
        console.log(`   CD35: ${stats.cd35_recupere} récupérés → ${stats.cd35_garde} gardés`);
        
        // CD56
        cd56Features.forEach(feature => {
            const converted = cd56ToFeature(feature);
            if (converted) {
                features.push(converted);
                stats.cd56_garde++;
            }
        });
        console.log(`   CD56: ${stats.cd56_recupere} récupérés → ${stats.cd56_garde} gardés`);
        
        const totalGarde = stats.grist_garde + stats.cd44_garde + stats.rennes_garde + stats.cd35_garde + stats.cd56_garde;
        const totalFiltre = totalBrut - totalGarde;
        
        console.log(`\n📊 Résumé:`);
        console.log(`   Total récupéré: ${totalBrut}`);
        console.log(`   Total gardé: ${totalGarde}`);
        console.log(`   Total filtré: ${totalFiltre}\n`);
        
        const geojson = {
            type: 'FeatureCollection',
            features: features,
            metadata: {
                generated: new Date().toISOString(),
                source: 'Fusion Grist 35 + CD44 + Rennes Métropole + CD35 Inondations + CD56',
                total_count: features.length,
                sources: {
                    grist_35: gristRecords.length,
                    cd44: cd44Records.length,
                    rennes_metropole: rennesMetroFeatures.length,
                    cd35_inondations: cd35InondationsFeatures.length,
                    cd56: cd56Features.length
                }
            }
        };
        
        fs.writeFileSync('signalements.geojson', JSON.stringify(geojson, null, 2));
        console.log('✅ Fichier signalements.geojson créé');
        
        const metadata = {
            lastUpdate: new Date().toISOString(),
            sources: {
                grist_35: gristRecords.length,
                cd44: cd44Records.length,
                rennes_metropole: rennesMetroFeatures.length,
                cd35_inondations: cd35InondationsFeatures.length,
                cd56: cd56Features.length,
                total: features.length
            },
            stats: {
                points: features.filter(f => f.geometry.type === 'Point').length,
                lines: features.filter(f => f.geometry.type === 'LineString').length,
                multilines: features.filter(f => f.geometry.type === 'MultiLineString').length,
                polygons: features.filter(f => f.geometry.type === 'Polygon').length,
                by_source: {
                    saisie_grist: features.filter(f => f.properties.source === 'Saisie Grist').length,
                    cd44: features.filter(f => f.properties.source === 'CD44').length,
                    rennes_metropole: features.filter(f => f.properties.source === 'Rennes Métropole').length,
                    cd35_inondations: features.filter(f => f.properties.source === 'CD35 Inondations').length,
                    cd56: features.filter(f => f.properties.source === 'CD56').length
                }
            }
        };
        
        fs.writeFileSync('metadata.json', JSON.stringify(metadata, null, 2));
        console.log('✅ Métadonnées créées');
        
        console.log('\n📊 Statistiques finales:');
        console.log(`   - Grist 35: ${gristRecords.length}`);
        console.log(`   - CD44: ${cd44Records.length}`);
        console.log(`   - Rennes Métropole: ${rennesMetroFeatures.length}`);
        console.log(`   - CD35 Inondations: ${cd35InondationsFeatures.length}`);
        console.log(`   - CD56: ${cd56Features.length}`);
        console.log(`   - Total features: ${features.length}`);
        console.log(`   - Points: ${metadata.stats.points}`);
        console.log(`   - LineStrings: ${metadata.stats.lines}`);
        console.log(`   - Polygons: ${metadata.stats.polygons}`);
        
    } catch (error) {
        console.error('❌ Erreur fusion:', error.message);
        process.exit(1);
    }
}

mergeSources();
