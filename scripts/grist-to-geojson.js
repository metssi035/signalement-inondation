const https = require('https');
const fs = require('fs');
const fetch = require('node-fetch');
const xml2js = require('xml2js');

const GRIST_DOC_ID = process.env.GRIST_DOC_ID;
const GRIST_API_KEY = process.env.GRIST_API_KEY;
const TABLE_ID = 'Signalements';

console.log('🚀 Démarrage de la fusion des 5 sources...\n');
console.log('   1. Grist 35 (signalements manuels)');
console.log('   2. CD44 (API REST)');
console.log('   3. Rennes Métropole (API REST)');
console.log('   4. CD35 Inondations (WFS XML)');
console.log('   5. CD56 (API REST ArcGIS)\n');

// =====================================================
// CONFIGURATION
// =====================================================

const CD35_WFS_CONFIG = {
    url: 'https://dservices1.arcgis.com/jGLANYlFVVx3nuxa/arcgis/services/Inondations_cd35/WFSServer',
    typeName: 'Inondations_cd35:Inondation',
    srsName: 'EPSG:2154'
};

const CD56_REST_URL = 'https://services.arcgis.com/4GFMPbPboxIs6KOG/arcgis/rest/services/TEST_INONDATION_V2/FeatureServer/0/query';

// ✅ FONCTION DE FORMATAGE DES DATES
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
            date = new Date(dateValue * 1000);
        } else {
            return '';
        }
        
        // Vérifier validité
        if (isNaN(date.getTime())) {
            return '';
        }
        
        // Format JJ/MM/AAAA HH:MM
        const day = String(date.getDate()).padStart(2, '0');
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const year = date.getFullYear();
        const hours = String(date.getHours()).padStart(2, '0');
        const minutes = String(date.getMinutes()).padStart(2, '0');
        
        return `${day}/${month}/${year} à ${hours}h${minutes}`;
        
    } catch (e) {
        return '';
    }
}

// =====================================================
// CONVERSION LAMBERT 93 → WGS84
// =====================================================

function convertLambert93ToWGS84(x, y) {
    const e = 0.08181919106;
    const a = 6378137.0;
    const lambda0 = 3 * Math.PI / 180;
    const phi0 = 46.5 * Math.PI / 180;
    const phi1 = 44 * Math.PI / 180;
    const phi2 = 49 * Math.PI / 180;
    const x0 = 700000;
    const y0 = 6600000;
    
    const lng = lambda0 + (x - x0) / (a * Math.cos(phi0) * 111320);
    const lat = phi0 + (y - y0) / (a * 111320);
    
    return [lng * 180 / Math.PI, lat * 180 / Math.PI];
}

// =====================================================
// PARSING WFS XML POUR CD35
// =====================================================

async function fetchWFSData(config, sourceName) {
    try {
        console.log(`🔗 [${sourceName}] Récupération via WFS...`);
        
        const wfsUrl = `${config.url}?` +
            `service=WFS&` +
            `version=2.0.0&` +
            `request=GetFeature&` +
            `typeNames=${config.typeName}&` +
            `srsName=${config.srsName}`;
        
        console.log(`   URL: ${wfsUrl.substring(0, 80)}...`);
        
        const response = await fetch(wfsUrl);
        
        if (!response.ok) {
            console.error(`❌ [${sourceName}] HTTP ${response.status}`);
            return [];
        }
        
        const xmlText = await response.text();
        console.log(`   Réponse XML reçue (${xmlText.length} caractères)`);
        
        const parser = new xml2js.Parser({
            explicitArray: false,
            ignoreAttrs: false,
            tagNameProcessors: [xml2js.processors.stripPrefix]
        });
        
        const result = await parser.parseStringPromise(xmlText);
        
        const features = [];
        const members = result.FeatureCollection?.member || [];
        const memberArray = Array.isArray(members) ? members : [members];
        
        console.log(`   ${memberArray.length} features trouvées`);
        
        memberArray.forEach(member => {
            try {
                const featureData = member.Inondation || member[Object.keys(member)[0]];
                
                if (!featureData) return;
                
                const properties = {};
                Object.keys(featureData).forEach(key => {
                    if (key !== 'geometry' && key !== 'shape') {
                        properties[key] = featureData[key];
                    }
                });
                
                let geometry = null;
                const geomField = featureData.geometry || featureData.shape || featureData.SHAPE;
                
                if (geomField) {
                    if (geomField.Point && geomField.Point.pos) {
                        const coords = geomField.Point.pos.split(' ');
                        const x = parseFloat(coords[0]);
                        const y = parseFloat(coords[1]);
                        const [lng, lat] = convertLambert93ToWGS84(x, y);
                        
                        geometry = {
                            type: 'Point',
                            coordinates: [lng, lat]
                        };
                    }
                    else if (geomField.LineString && geomField.LineString.posList) {
                        const coords = geomField.LineString.posList.split(' ');
                        const coordinates = [];
                        for (let i = 0; i < coords.length; i += 2) {
                            const x = parseFloat(coords[i]);
                            const y = parseFloat(coords[i + 1]);
                            const [lng, lat] = convertLambert93ToWGS84(x, y);
                            coordinates.push([lng, lat]);
                        }
                        geometry = {
                            type: 'LineString',
                            coordinates: coordinates
                        };
                    }
                }
                
                if (geometry) {
                    features.push({
                        type: 'Feature',
                        geometry: geometry,
                        properties: properties
                    });
                }
                
            } catch (e) {
                console.warn(`   ⚠️ Erreur parsing feature:`, e.message);
            }
        });
        
        console.log(`✅ [${sourceName}] ${features.length} features parsées avec succès`);
        return features;
        
    } catch (error) {
        console.error(`❌ [${sourceName}]`, error.message);
        return [];
    }
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

// Récupérer Rennes Métropole
async function fetchRennesMetropoleData() {
    try {
        console.log('🔗 [Rennes Métropole] Récupération...');
        const response = await fetch(
            'https://data.rennesmetropole.fr/api/explore/v2.1/catalog/datasets/travaux_1_jour/records?limit=100'
        );
        const data = await response.json();
        const records = data.results || [];
        console.log(`✅ [Rennes Métropole] ${records.length} records`);
        return records;
    } catch (error) {
        console.error('❌ [Rennes Métropole]', error.message);
        return [];
    }
}

// Récupérer CD35 Inondations (WFS)
async function fetchCD35InondationsData() {
    return fetchWFSData(CD35_WFS_CONFIG, 'CD35 Inondations');
}

// Récupérer CD56 (API REST ArcGIS)
async function fetchCD56Data() {
    try {
        console.log(`🔗 [CD56] Récupération via API REST ArcGIS...`);
        
        const queryParams = new URLSearchParams({
            where: '1=1',
            outFields: '*',
            f: 'geojson',
            outSR: '4326'
        });
        
        const url = `${CD56_REST_URL}?${queryParams.toString()}`;
        console.log(`   URL: ${url.substring(0, 80)}...`);
        
        const response = await fetch(url);
        
        if (!response.ok) {
            console.error(`❌ [CD56] HTTP ${response.status}`);
            return [];
        }
        
        const geojson = await response.json();
        console.log(`   Réponse GeoJSON reçue`);
        
        const features = geojson.features || [];
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
        
        return {
            type: 'Feature',
            geometry: geometry,
            properties: {
                id: record.id,
                source: 'Grist 35',
                route: record.fields.Route || '',
                commune: record.fields.Commune || '',
                etat: record.fields.Type_coupure || 'Route fermée',
                cause: cause || 'Inondation',
                statut: statut,
                statut_actif: statut === 'Actif',
                statut_resolu: statut === 'Résolu',
                type_coupure: record.fields.Type_coupure || '',
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

// Convertir CD44
function cd44ToFeature(item) {
    try {
        const geometry = {
            type: 'Point',
            coordinates: [item.longitude, item.latitude]
        };
        
        const route = Array.isArray(item.ligne2) ? item.ligne2.join(' / ') : (item.ligne2 || 'Route');
        const statut = 'Actif';
        
        return {
            type: 'Feature',
            geometry: geometry,
            properties: {
                id: `cd44-${item.recordid}`,
                source: 'CD44',
                route: route,
                commune: item.ligne3 || 'Commune',
                etat: item.type || 'Route fermée',
                cause: item.nature || '',
                statut: statut,
                statut_actif: true,
                statut_resolu: false,
                type_coupure: item.type || '',
                sens_circulation: '',
                commentaire: item.ligne1 || '',
                date_debut: formatDate(item.datepublication),
                date_fin: '',
                date_saisie: formatDate(item.datepublication),
                gestionnaire: 'CD44'
            }
        };
    } catch (e) {
        return null;
    }
}

// Convertir Rennes Métropole
function rennesMetropoleToFeatures(item) {
    try {
        let geometry = null;
        
        if (item.geo_shape && item.geo_shape.geometry) {
            geometry = item.geo_shape.geometry;
        } else if (item.geo_point_2d) {
            geometry = {
                type: 'Point',
                coordinates: [item.geo_point_2d.lon, item.geo_point_2d.lat]
            };
        }
        
        if (!geometry) return [];
        
        const statut = 'Actif';
        
        return [{
            type: 'Feature',
            geometry: geometry,
            properties: {
                id: `rm-${item.recordid}`,
                source: 'Rennes Métropole',
                route: item.localisation || item.rue || '',
                commune: item.commune || 'Rennes',
                etat: 'Route fermée',
                cause: 'Travaux',
                statut: statut,
                statut_actif: true,
                statut_resolu: false,
                type_coupure: item.type || '',
                sens_circulation: '',
                commentaire: item.libelle || '',
                date_debut: formatDate(item.date_deb),
                date_fin: formatDate(item.date_fin),
                date_saisie: formatDate(item.date_deb),
                gestionnaire: 'Rennes Métropole'
            }
        }];
        
    } catch (e) {
        return [];
    }
}

// Convertir CD35 Inondations
function cd35InondationsToFeature(feature) {
    try {
        const geometry = feature.geometry;
        if (!geometry) return null;
        
        const props = feature.properties || {};
        
        const etatCirculation = props.etat_circulation || props.etatCirculation || 'Non spécifié';
        const statut = etatCirculation.toLowerCase().includes('fermée') || 
                      etatCirculation.toLowerCase().includes('fermé') ? 'Actif' : 'Actif';
        
        return {
            type: 'Feature',
            geometry: geometry,
            properties: {
                id: `cd35-inond-${props.OBJECTID || props.objectid || feature.id || ''}`,
                source: 'CD35 Inondations',
                route: props.route || props.Route || '',
                commune: props.commune || props.Commune || '',
                etat: etatCirculation,
                cause: 'Inondation',
                statut: statut,
                statut_actif: statut === 'Actif',
                statut_resolu: statut === 'Résolu',
                type_coupure: 'Totale',
                sens_circulation: '',
                commentaire: props.lieu_dit || props.lieuDit || '',
                date_debut: '',
                date_fin: '',
                date_saisie: new Date().toISOString(),
                gestionnaire: 'CD35',
                agence: props.agence || props.Agence || '',
                pr_debut: props.PR_debut || props.PRDebut || '',
                pr_fin: props.PR_fin || props.PRFin || ''
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
        
        const conditionsCirculation = props.conditions_circulation || props.conditionsCirculation || '';
        if (conditionsCirculation.toUpperCase() !== 'COUPÉE') {
            return null;
        }
        
        const statut = props.statut || props.Statut || 'Actif';
        
        return {
            type: 'Feature',
            geometry: geometry,
            properties: {
                id: `cd56-${props.OBJECTID || props.objectid || props.id || feature.id || ''}`,
                source: 'CD56',
                route: props.route || props.Route || props.rd || '',
                commune: props.commune || props.Commune || '',
                etat: 'Route fermée',
                cause: props.cause || props.Cause || 'Inondation',
                statut: statut,
                statut_actif: statut.toLowerCase() === 'actif',
                statut_resolu: statut.toLowerCase() === 'résolu',
                type_coupure: props.type_coupure || props.typeCoupure || '',
                sens_circulation: props.sens || props.Sens || '',
                commentaire: props.commentaire || props.Commentaire || props.description || '',
                date_debut: formatDate(props.date_debut || props.dateDebut || props.date),
                date_fin: formatDate(props.date_fin || props.dateFin),
                date_saisie: formatDate(props.date_creation || props.dateCreation || props.date),
                gestionnaire: 'CD56',
                conditions_circulation: 'COUPÉE'
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
        
        const [gristRecords, cd44Records, rennesMetropoleRecords, cd35InondationsFeatures, cd56Features] = await Promise.all([
            fetchGristData(),
            fetchCD44Data(),
            fetchRennesMetropoleData(),
            fetchCD35InondationsData(),
            fetchCD56Data()
        ]);
        
        const totalBrut = gristRecords.length + cd44Records.length + rennesMetropoleRecords.length + 
                         cd35InondationsFeatures.length + cd56Features.length;
        console.log(`\n📊 Total brut: ${totalBrut} records\n`);
        
        let features = [];
        
        gristRecords.forEach(record => {
            const feature = gristToFeature(record);
            if (feature) features.push(feature);
        });
        
        cd44Records.forEach(item => {
            const feature = cd44ToFeature(item);
            if (feature) features.push(feature);
        });
        
        rennesMetropoleRecords.forEach(item => {
            const rmsFeatures = rennesMetropoleToFeatures(item);
            features.push(...rmsFeatures);
        });
        
        cd35InondationsFeatures.forEach(feature => {
            const converted = cd35InondationsToFeature(feature);
            if (converted) features.push(converted);
        });
        
        cd56Features.forEach(feature => {
            const converted = cd56ToFeature(feature);
            if (converted) features.push(converted);
        });
        
        console.log(`✅ ${features.length} features créées\n`);
        
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
                    rennes_metropole: rennesMetropoleRecords.length,
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
                rennes_metropole: rennesMetropoleRecords.length,
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
                    grist_35: features.filter(f => f.properties.source === 'Grist 35').length,
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
        console.log(`   - Rennes Métropole: ${rennesMetropoleRecords.length}`);
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
