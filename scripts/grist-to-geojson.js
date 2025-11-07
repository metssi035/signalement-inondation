const https = require('https');
const fs = require('fs');
const fetch = require('node-fetch');

const GRIST_DOC_ID = process.env.GRIST_DOC_ID;
const GRIST_API_KEY = process.env.GRIST_API_KEY;
const TABLE_ID = 'Signalements';

// Configuration CD56 WFS
const CD56_WFS_CONFIG = {
    baseUrl: 'https://dservices.arcgis.com/4GFMPbPboxIs6KOG/arcgis/services/TEST_INONDATION_V2/WFSServer',
    service: 'WFS',
    version: '2.0.0',
    request: 'GetFeature',
    typeName: 'TEST_INONDATION_V2:Routes_Concernees',
    outputFormat: 'geojson',  // ✅ Changé de application/json à geojson
    maxFeatures: 200,  // Normalement 116 entités totales
    // ⚠️ FILTRE DÉSACTIVÉ - Récupère toutes les routes, filtre côté client
    // cqlFilter: "conditions_circulation='COUPÉE'"
};

console.log('🚀 Démarrage de la fusion des 4 sources...\n');

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

// 🆕 Récupérer CD56 via WFS
async function fetchCD56Data() {
async function fetchCD56Data() {
    console.log('🔗 [CD56] Récupération via WFS...');
    
    // Liste des formats à essayer dans l'ordre
    const formats = ['geojson', 'application/json', 'json', 'application/geo+json'];
    
    for (const format of formats) {
        try {
            console.log(`   Tentative avec outputFormat: ${format}`);
            
            // Construction de l'URL WFS
            let wfsUrl = `${CD56_WFS_CONFIG.baseUrl}?` +
                `service=${CD56_WFS_CONFIG.service}&` +
                `version=${CD56_WFS_CONFIG.version}&` +
                `request=${CD56_WFS_CONFIG.request}&` +
                `typeNames=${CD56_WFS_CONFIG.typeName}&` +
                `outputFormat=${encodeURIComponent(format)}&` +
                `count=${CD56_WFS_CONFIG.maxFeatures}`;
            
            // Ajouter le filtre CQL si présent
            if (CD56_WFS_CONFIG.cqlFilter) {
                wfsUrl += `&CQL_FILTER=${encodeURIComponent(CD56_WFS_CONFIG.cqlFilter)}`;
                console.log(`   📌 Filtre: ${CD56_WFS_CONFIG.cqlFilter}`);
            }
            
            const response = await fetch(wfsUrl, {
                headers: {
                    'User-Agent': 'Mozilla/5.0',
                    'Accept': 'application/json, application/geo+json, application/vnd.geo+json'
                }
            });
            
            console.log(`   Statut HTTP: ${response.status}`);
            
            if (!response.ok) {
                console.log(`   ❌ Échec avec ${format} (HTTP ${response.status})`);
                continue; // Essayer le format suivant
            }
            
            const text = await response.text();
            
            // Vérifier si c'est du XML au lieu de JSON
            if (text.trim().startsWith('<')) {
                console.log(`   ❌ ${format} retourne du XML, essai suivant...`);
                continue;
            }
            
            const data = JSON.parse(text);
            const features = data.features || [];
            
            console.log(`✅ [CD56] ${features.length} features (format: ${format})`);
            
            // Debug: afficher les propriétés du premier élément
            if (features.length > 0) {
                console.log('   📋 Exemple de propriétés CD56:');
                const props = features[0].properties || {};
                Object.keys(props).slice(0, 10).forEach(key => {
                    console.log(`      - ${key}: ${props[key]}`);
                });
                
                // Chercher le champ qui pourrait indiquer l'état
                const etatFields = ['conditions_circulation', 'etat', 'statut', 'type', 'state'];
                etatFields.forEach(field => {
                    if (props[field]) {
                        console.log(`   ⭐ Champ "${field}" trouvé: ${props[field]}`);
                    }
                });
            }
            
            return features;
            
        } catch (error) {
            console.log(`   ❌ Erreur avec ${format}: ${error.message}`);
            continue;
        }
    }
    
    // Si tous les formats ont échoué
    console.error('❌ [CD56] Impossible de récupérer les données avec aucun format');
    console.error('   Formats essayés: ' + formats.join(', '));
    console.error('   Le serveur WFS peut nécessiter un format spécifique ou être indisponible');
    return [];
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

// 🆕 Convertir CD56
function cd56ToFeature(feature) {
    try {
        // La feature vient déjà avec une geometry du WFS
        const geometry = feature.geometry;
        
        if (!geometry) return null;
        
        const props = feature.properties || {};
        
        // 🔍 FILTRE EXACT : conditions_circulation = "COUPÉE"
        if (props.conditions_circulation !== 'COUPÉE') {
            return null;  // On ignore cette route
        }
        
        // Mapping des propriétés CD56 vers notre format unifié
        const statut = props.statut || props.etat || 'Actif';
        
        return {
            type: 'Feature',
            geometry: geometry,
            properties: {
                id: `cd56-${props.objectid || props.OBJECTID || props.id || feature.id}`,
                source: 'CD56',
                route: props.route || props.rd || props.voie || props.axe || '',
                commune: props.commune || props.ville || '',
                etat: 'Route fermée',
                cause: props.cause || props.nature || 'Inondation',
                statut: statut,
                statut_actif: statut.toLowerCase() === 'actif' || statut.toLowerCase() === 'en cours',
                statut_resolu: statut.toLowerCase() === 'résolu' || statut.toLowerCase() === 'terminé',
                type_coupure: props.type_coupure || props.type || 'Coupure totale',
                sens_circulation: props.sens || props.sens_circulation || '',
                commentaire: props.commentaire || props.description || props.libelle || '',
                date_debut: formatDate(props.date_debut || props.date_deb || props.date),
                date_fin: formatDate(props.date_fin),
                date_saisie: formatDate(props.date_creation || props.date_saisie || props.date),
                gestionnaire: 'CD56',
                conditions_circulation: 'COUPÉE',
                
                // Propriétés supplémentaires spécifiques CD56
                cd56_raw: {
                    ...props
                }
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
        
        const [gristRecords, cd44Records, rennesMetropoleRecords, cd56Features] = await Promise.all([
            fetchGristData(),
            fetchCD44Data(),
            fetchRennesMetropoleData(),
            fetchCD56Data()
        ]);
        
        console.log(`\n📊 Total brut: ${gristRecords.length + cd44Records.length + rennesMetropoleRecords.length + cd56Features.length} records\n`);
        
        let features = [];
        
        // Conversion Grist
        console.log('🔄 Conversion Grist 35...');
        gristRecords.forEach(record => {
            const feature = gristToFeature(record);
            if (feature) features.push(feature);
        });
        
        // Conversion CD44
        console.log('🔄 Conversion CD44...');
        cd44Records.forEach(item => {
            const feature = cd44ToFeature(item);
            if (feature) features.push(feature);
        });
        
        // Conversion Rennes Métropole
        console.log('🔄 Conversion Rennes Métropole...');
        rennesMetropoleRecords.forEach(item => {
            const rmsFeatures = rennesMetropoleToFeatures(item);
            features.push(...rmsFeatures);
        });
        
        // 🆕 Conversion CD56
        console.log('🔄 Conversion CD56...');
        cd56Features.forEach(feature => {
            const converted = cd56ToFeature(feature);
            if (converted) features.push(converted);
        });
        
        console.log(`\n✅ ${features.length} features créées\n`);
        
        const geojson = {
            type: 'FeatureCollection',
            features: features,
            metadata: {
                generated: new Date().toISOString(),
                source: 'Fusion Grist 35 + CD44 + Rennes Métropole + CD56',
                total_count: features.length,
                sources: {
                    grist_35: gristRecords.length,
                    cd44: cd44Records.length,
                    rennes_metropole: rennesMetropoleRecords.length,
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
        console.log(`   - CD56: ${cd56Features.length}`);
        console.log(`   - Total features: ${features.length}`);
        console.log(`   - Points: ${metadata.stats.points}`);
        console.log(`   - LineStrings: ${metadata.stats.lines}`);
        console.log(`   - MultiLineStrings: ${metadata.stats.multilines}`);
        console.log(`   - Polygons: ${metadata.stats.polygons}`);
        
        // Sauvegarde d'un exemple de données CD56 pour analyse
        if (cd56Features.length > 0) {
            const cd56Sample = {
                count: cd56Features.length,
                firstFeature: cd56Features[0],
                allProperties: cd56Features.map(f => Object.keys(f.properties || {}))
            };
            fs.writeFileSync('cd56_sample.json', JSON.stringify(cd56Sample, null, 2));
            console.log('   - Échantillon CD56 sauvegardé dans cd56_sample.json');
        }
        
    } catch (error) {
        console.error('❌ Erreur fusion:', error.message);
        process.exit(1);
    }
}

mergeSources();
