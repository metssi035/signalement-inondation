/**
 * Fusion des signalements (CD44, Grist35, Rennes Métropole, CD35, CD56)
 * Génère un fichier GeoJSON consolidé
 */

const fs = require('fs');
const fetch = require('node-fetch');
const proj4 = require('proj4');
const { parseStringPromise } = require('xml2js');

// Définition de la projection Lambert 93 (EPSG:2154)
proj4.defs("EPSG:2154", "+proj=lcc +lat_1=49 +lat_2=44 +lat_0=46.5 +lon_0=3 +x_0=700000 +y_0=6600000 +ellps=GRS80 +units=m +no_defs");

// URLs des différentes sources
const urlCD44 = 'https://example.com/cd44.xml'; // ton URL CD44 existante
const urlGrist35 = 'https://example.com/grist35.xml'; // ton URL existante
const urlRM = 'https://example.com/rennes_metropole.xml'; // ton URL existante
const urlCD35 = 'https://dservices1.arcgis.com/jGLANYlFVVx3nuxa/arcgis/services/Inondations_cd35/WFSServer?service=WFS&version=2.0.0&request=GetFeature&typeNames=Inondations_cd35:Inondation&srsName=EPSG:2154';
const urlCD56 = 'https://example.com/cd56.xml'; // ton URL existante

// Fonction générique pour chaque source
async function processSource(name, url, parser) {
  try {
    console.log(`\n🔹 [${name}] Récupération des données...`);
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
    const text = await res.text();
    console.log(`   Réponse XML reçue (${text.length} caractères)`);
    const feats = await parser(text);
    console.log(`✅ [${name}] ${feats.length} features parsées avec succès`);
    return feats;
  } catch (err) {
    console.error(`❌ [${name}] Erreur:`, err.message);
    return [];
  }
}

// =======================================================================
// PARSEURS EXISTANTS (exemples placeholders, à remplacer par tes vrais)
// =======================================================================

async function parseCD44(xmlText) {
  const json = await parseStringPromise(xmlText, { explicitArray: false });
  const features = []; // ta logique CD44 existante ici
  return features;
}

async function parseGrist35(xmlText) {
  const json = await parseStringPromise(xmlText, { explicitArray: false });
  const features = []; // ta logique Grist35 existante ici
  return features;
}

async function parseRM(xmlText) {
  const json = await parseStringPromise(xmlText, { explicitArray: false });
  const features = []; // ta logique Rennes Métropole existante ici
  return features;
}

async function parseCD56(xmlText) {
  const json = await parseStringPromise(xmlText, { explicitArray: false });
  const features = []; // ta logique CD56 existante ici
  return features;
}

// =======================================================================
// 💧 NOUVEAU PARSEUR CD35
// =======================================================================

async function parseCD35(xmlText) {
  const json = await parseStringPromise(xmlText, { explicitArray: false });
  const features = [];

  // fonction récursive pour trouver les balises pos ou gml:pos
  function findPositions(obj) {
    const results = [];
    if (!obj || typeof obj !== 'object') return results;
    for (const key of Object.keys(obj)) {
      const val = obj[key];
      if (typeof val === 'string' && (key.endsWith(':pos') || key === 'pos')) {
        results.push(val);
      } else if (typeof val === 'object') {
        results.push(...findPositions(val));
      }
    }
    return results;
  }

  const positions = findPositions(json);
  console.log(`   CD35 → ${positions.length} positions trouvées`);

  for (const pos of positions) {
    const coords = pos.trim().split(/\s+/);
    if (coords.length < 2) continue;
    const x = parseFloat(coords[0]);
    const y = parseFloat(coords[1]);
    if (isNaN(x) || isNaN(y)) continue;

    const [lng, lat] = proj4("EPSG:2154", "EPSG:4326", [x, y]);
    features.push({
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [lng, lat] },
      properties: { source: 'CD35' }
    });
  }

  return features;
}

// =======================================================================
// MAIN
// =======================================================================

async function main() {
  const allFeatures = [];

  // Ajoute ici tes appels existants (inchangés)
  const cd44 = await processSource('CD44', urlCD44, parseCD44);
  const grist35 = await processSource('Grist 35', urlGrist35, parseGrist35);
  const rennes = await processSource('Rennes Métropole', urlRM, parseRM);

  // 👉 NOUVELLE LIGNE CD35
  const cd35 = await processSource('CD35', urlCD35, parseCD35);

  const cd56 = await processSource('CD56', urlCD56, parseCD56);

  allFeatures.push(...cd44, ...grist35, ...rennes, ...cd35, ...cd56);

  console.log(`\n📊 Total brut: ${allFeatures.length} records`);

  const geojson = {
    type: 'FeatureCollection',
    features: allFeatures
  };

  const outputPath = './output/signalements_fusion.geojson';
  fs.mkdirSync('./output', { recursive: true });
  fs.writeFileSync(outputPath, JSON.stringify(geojson, null, 2), 'utf8');
  console.log(`\n✅ Fichier GeoJSON écrit : ${outputPath}`);
}

// Lancement
main();
