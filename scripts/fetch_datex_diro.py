#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de récupération DATEX II - DIR Ouest
Récupère les événements routiers actifs de la DIR Ouest (Bretagne/Pays de la Loire)
"""

import requests
import json
from lxml import etree
from datetime import datetime
import os

# Configuration
DATEX_URL = 'https://tipi.bison-fute.gouv.fr/bison-fute-ouvert/publicationsDIR/Evenementiel-DIR/grt/RRN/content.xml'
OUTPUT_FILE = 'data/datex-diro.geojson'
STATS_FILE = 'data/datex-diro-stats.txt'

# Namespaces XML
NS = {
    'soap': 'http://www.w3.org/2003/05/soap-envelope',
    'ns2': 'http://datex2.eu/schema/2/2_0',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
}

def fetch_xml():
    """Récupère le XML depuis Bison Futé"""
    print(f"📡 Récupération du XML depuis {DATEX_URL}")
    
    try:
        response = requests.get(DATEX_URL, timeout=30)
        response.raise_for_status()
        print(f"✅ XML récupéré ({len(response.content)} octets)")
        return response.content
    except Exception as e:
        print(f"❌ Erreur de récupération: {e}")
        raise

def parse_datex(xml_content):
    """Parse le XML DATEX II et extrait les événements DIR Ouest actifs"""
    print("🔍 Parsing du XML...")
    
    try:
        root = etree.fromstring(xml_content)
    except Exception as e:
        print(f"❌ Erreur de parsing XML: {e}")
        raise
    
    situations = root.findall('.//ns2:situation', NS)
    print(f"📊 {len(situations)} situations trouvées au total")
    
    now = datetime.now()
    features = []
    stats = {
        'total_situations': len(situations),
        'dir_ouest': 0,
        'actifs': 0,
        'par_severite': {},
        'par_type': {},
        'sans_coords': 0
    }
    
    for situation in situations:
        sit_id = situation.get('id')
        
        # Récupérer la sévérité
        severity_elem = situation.find('.//ns2:overallSeverity', NS)
        severity = severity_elem.text if severity_elem is not None else 'medium'
        
        for record in situation.findall('.//ns2:situationRecord', NS):
            
            # Vérifier la source (DIR Ouest uniquement)
            source_elem = record.find('.//ns2:sourceIdentification', NS)
            if source_elem is None:
                continue
            
            source = source_elem.text
            
            # Filtrer DIR Ouest / DIRO
            if 'DIR Ouest' not in source and 'DIRO' not in source:
                continue
            
            stats['dir_ouest'] += 1
            
            # Vérifier si l'événement est actif
            start_elem = record.find('.//ns2:overallStartTime', NS)
            if start_elem is None:
                continue
            
            try:
                start_date = datetime.fromisoformat(start_elem.text.replace('+02:00', '').replace('+01:00', ''))
            except:
                continue
            
            # Vérifier date de fin
            end_elem = record.find('.//ns2:overallEndTime', NS)
            if end_elem is not None:
                try:
                    end_date = datetime.fromisoformat(end_elem.text.replace('+02:00', '').replace('+01:00', ''))
                    if now > end_date:
                        continue  # Événement terminé
                except:
                    pass
            
            if start_date > now:
                continue  # Événement pas encore commencé
            
            stats['actifs'] += 1
            
            # Extraire coordonnées GPS
            lat_elems = record.findall('.//ns2:latitude', NS)
            lon_elems = record.findall('.//ns2:longitude', NS)
            
            if not lat_elems or not lon_elems:
                stats['sans_coords'] += 1
                continue
            
            try:
                lat = float(lat_elems[0].text)
                lon = float(lon_elems[0].text)
            except (ValueError, AttributeError):
                stats['sans_coords'] += 1
                continue
            
            # Extraire les informations
            road_elem = record.find('.//ns2:roadNumber', NS)
            road = road_elem.text if road_elem is not None else 'N/A'
            
            # Type d'événement
            record_type = record.get('{http://www.w3.org/2001/XMLSchema-instance}type', 'N/A')
            record_type = record_type.replace('ns2:', '')
            
            # Extraire descriptions
            comments = []
            for comment_elem in record.findall('.//ns2:generalPublicComment/ns2:comment/ns2:values/ns2:value[@lang="fr"]', NS):
                if comment_elem.text:
                    comments.append(comment_elem.text)
            
            description = ' | '.join(comments) if comments else 'Pas de description'
            
            # Type de problème spécifique
            problem_type = 'Autre'
            problem_mapping = {
                'roadClosed': 'Route fermée',
                'laneClosures': 'Voie fermée',
                'weightRestrictionInOperation': 'Restriction poids',
                'abnormalTrafficType': 'Trafic anormal',
                'obstructionType': 'Obstruction'
            }
            
            record_text = etree.tostring(record, encoding='unicode')
            for key, label in problem_mapping.items():
                if key in record_text:
                    problem_type = label
                    break
            
            # Stats
            stats['par_severite'][severity] = stats['par_severite'].get(severity, 0) + 1
            stats['par_type'][record_type] = stats['par_type'].get(record_type, 0) + 1
            
            # Créer la feature GeoJSON
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat]
                },
                "properties": {
                    "id": sit_id,
                    "source": source,
                    "road": road,
                    "type": record_type,
                    "problem": problem_type,
                    "severity": severity,
                    "description": description[:200],  # Limiter la longueur
                    "start_date": start_elem.text,
                    "end_date": end_elem.text if end_elem is not None else None,
                    "updated_at": datetime.now().isoformat()
                }
            }
            
            features.append(feature)
    
    print(f"✅ {len(features)} événements DIR Ouest actifs extraits")
    return features, stats

def create_geojson(features, stats):
    """Crée le fichier GeoJSON"""
    geojson = {
        "type": "FeatureCollection",
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "source": "DATEX II - Bison Futé",
            "filter": "DIR Ouest (Bretagne / Pays de la Loire) - Événements actifs",
            "count": len(features),
            "statistics": stats
        },
        "features": features
    }
    
    # Créer le dossier data si nécessaire
    os.makedirs('data', exist_ok=True)
    
    # Sauvegarder le GeoJSON
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, indent=2, ensure_ascii=False)
    
    print(f"💾 Fichier sauvegardé: {OUTPUT_FILE}")
    
    # Créer le fichier de stats
    with open(STATS_FILE, 'w', encoding='utf-8') as f:
        f.write(f"📊 STATISTIQUES DATEX II DIR OUEST\n")
        f.write(f"=" * 50 + "\n\n")
        f.write(f"🕐 Généré le: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"📍 Zone: DIR Ouest (Bretagne / Pays de la Loire)\n\n")
        f.write(f"Situations totales: {stats['total_situations']}\n")
        f.write(f"Situations DIR Ouest: {stats['dir_ouest']}\n")
        f.write(f"✅ Événements actifs: {len(features)}\n")
        f.write(f"⚠️  Sans coordonnées: {stats['sans_coords']}\n\n")
        
        if stats['par_severite']:
            f.write("Par sévérité:\n")
            for sev, count in sorted(stats['par_severite'].items()):
                f.write(f"  - {sev}: {count}\n")
        
        if stats['par_type']:
            f.write("\nPar type:\n")
            for typ, count in sorted(stats['par_type'].items(), key=lambda x: x[1], reverse=True):
                f.write(f"  - {typ}: {count}\n")
    
    print(f"📈 Stats sauvegardées: {STATS_FILE}")

def main():
    """Fonction principale"""
    print("=" * 60)
    print("🚗 RÉCUPÉRATION DATEX II - DIR OUEST")
    print("=" * 60)
    
    try:
        # Récupérer le XML
        xml_content = fetch_xml()
        
        # Parser et filtrer
        features, stats = parse_datex(xml_content)
        
        # Créer le GeoJSON
        create_geojson(features, stats)
        
        print("\n" + "=" * 60)
        print("✅ SUCCÈS - Données mises à jour")
        print("=" * 60)
        
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"❌ ERREUR: {e}")
        print("=" * 60)
        raise

if __name__ == '__main__':
    main()
