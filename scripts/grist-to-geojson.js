// ✅ Fonction pour formater les dates
function formatDate(dateString) {
    if (!dateString) return '';
    
    try {
        // Si c'est un timestamp (nombre)
        if (!isNaN(dateString)) {
            const date = new Date(parseFloat(dateString) * 1000);
            return date.toLocaleDateString('fr-FR', {
                day: '2-digit',
                month: '2-digit',
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
            });
        }
        
        // Si c'est déjà une date ISO
        const date = new Date(dateString);
        if (!isNaN(date.getTime())) {
            return date.toLocaleDateString('fr-FR', {
                day: '2-digit',
                month: '2-digit',
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
            });
        }
        
        // Sinon retourner tel quel
        return dateString;
    } catch (e) {
        return dateString;
    }
}

// Convertir Grist (MODIFIÉ)
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
                statut: record.fields.Statut || 'Actif',
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

// Convertir CD44 (MODIFIÉ)
function cd44ToFeature(item) {
    try {
        const geometry = {
            type: 'Point',
            coordinates: [item.longitude, item.latitude]
        };
        
        const route = Array.isArray(item.ligne2) ? item.ligne2.join(' / ') : (item.ligne2 || 'Route');
        
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
                statut: 'Actif',
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

// Convertir Rennes Métropole (MODIFIÉ)
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
                statut: 'Actif',
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
