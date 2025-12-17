#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de récupération DATEX II - Inondations DIR Ouest
Récupère les inondations (actives ET terminées) de la DIR Ouest (Bretagne/Pays de la Loire)

Ce script interroge l'API publique Bison Futé pour récupérer les événements routiers
au format DATEX II (standard européen d'échange de données routières), puis filtre
et transforme ces données en format GeoJSON exploitable.

Auteur: [À compléter]
Date: [À compléter]
Version: 1.0
"""

import requests
import json
from lxml import etree
from datetime import datetime
import os

# ============================================================================
# CONFIGURATION GLOBALE
# ============================================================================

# URL de l'API DATEX II de Bison Futé pour les événements routiers de la DIR Ouest
DATEX_URL = 'https://tipi.bison-fute.gouv.fr/bison-fute-ouvert/publicationsDIR/Evenementiel-DIR/grt/RRN/content.xml'

# Chemin du fichier de sortie GeoJSON contenant les inondations
OUTPUT_FILE = 'data/inondations-diro.geojson'

# Chemin du fichier de statistiques texte
STATS_FILE = 'data/inondations-diro-stats.txt'

# Définition des namespaces XML utilisés dans le format DATEX II
# Ces namespaces sont nécessaires pour naviguer dans la structure XML
NS = {
    'soap': 'http://www.w3.org/2003/05/soap-envelope',
    'ns2': 'http://datex2.eu/schema/2/2_0',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
}


# ============================================================================
# FONCTIONS DE RÉCUPÉRATION DE DONNÉES
# ============================================================================

def fetch_xml():
    """
    Récupère le flux XML DATEX II depuis l'API Bison Futé.
    
    Cette fonction effectue une requête HTTP GET vers l'API publique
    et retourne le contenu XML brut.
    
    Returns:
        bytes: Contenu XML brut de la réponse
        
    Raises:
        requests.exceptions.RequestException: En cas d'erreur de connexion ou HTTP
        Exception: Pour toute autre erreur lors de la récupération
    """
    print(f"Récupération du XML depuis {DATEX_URL}")
    
    try:
        # Requête HTTP avec timeout de 30 secondes pour éviter les blocages
        response = requests.get(DATEX_URL, timeout=30)
        
        # Vérifie que la requête a réussi (code HTTP 2xx)
        response.raise_for_status()
        
        print(f"XML récupéré avec succès ({len(response.content)} octets)")
        return response.content
        
    except Exception as e:
        print(f"Erreur lors de la récupération du XML: {e}")
        raise


# ============================================================================
# FONCTIONS DE PARSING ET FILTRAGE
# ============================================================================

def parse_datex(xml_content):
    """
    Parse le XML DATEX II et extrait les inondations de la DIR Ouest.
    
    Cette fonction applique une série de filtres pour extraire uniquement
    les événements pertinents :
    1. Filtre par source : DIR Ouest / DIRO uniquement
    2. Filtre par type : EnvironmentalObstruction
    3. Filtre par sous-type : flooding ou flashFloods
    
    Args:
        xml_content (bytes): Contenu XML brut à parser
        
    Returns:
        tuple: (liste de features GeoJSON, dictionnaire de statistiques)
        - features (list): Liste des événements au format GeoJSON Feature
        - stats (dict): Statistiques sur le parsing et le filtrage
        
    Raises:
        etree.XMLSyntaxError: Si le XML est mal formé
        Exception: Pour toute autre erreur de parsing
    """
    print("Début du parsing XML...")
    
    # Parse le contenu XML avec lxml
    try:
        root = etree.fromstring(xml_content)
    except Exception as e:
        print(f"Erreur lors du parsing XML: {e}")
        raise
    
    # Récupération de toutes les situations (événements) dans le flux
    situations = root.findall('.//ns2:situation', NS)
    print(f"Nombre total de situations trouvées : {len(situations)}")
    
    # Heure actuelle pour déterminer si un événement est encore actif
    now = datetime.now()
    
    # Liste qui contiendra les features GeoJSON à exporter
    features = []
    
    # Initialisation du dictionnaire de statistiques pour le reporting
    stats = {
        'total_situations': len(situations),        # Nombre total d'événements dans le flux
        'dir_ouest': 0,                              # Événements de la DIR Ouest
        'environmental_obstruction': 0,              # Type EnvironmentalObstruction
        'inondations': 0,                            # Inondations trouvées après tous les filtres
        'actives': 0,                                # Inondations encore en cours
        'terminees': 0,                              # Inondations terminées
        'par_severite': {},                          # Répartition par niveau de sévérité
        'par_subtype': {},                           # Répartition par sous-type (flooding/flashFloods)
        'sans_coords': 0                             # Événements sans coordonnées GPS
    }
    
    # Parcours de chaque situation dans le flux XML
    for situation in situations:
        # Récupération de l'identifiant unique de la situation
        sit_id = situation.get('id')
        
        # Récupération du niveau de sévérité global de la situation
        # Valeur par défaut : 'medium' si non spécifié
        severity_elem = situation.find('.//ns2:overallSeverity', NS)
        severity = severity_elem.text if severity_elem is not None else 'medium'
        
        # Une situation peut contenir plusieurs enregistrements (situationRecord)
        for record in situation.findall('.//ns2:situationRecord', NS):
            
            # ----------------------------------------------------------------
            # FILTRE 1 : Vérification de la source (DIR Ouest uniquement)
            # ----------------------------------------------------------------
            # On ne garde que les événements provenant de la DIR Ouest
            source_elem = record.find('.//ns2:sourceIdentification', NS)
            if source_elem is None:
                continue
            
            source = source_elem.text
            
            # Filtrage sur les identifiants DIR Ouest / DIRO
            if 'DIR Ouest' not in source and 'DIRO' not in source:
                continue
            
            stats['dir_ouest'] += 1
            
            # ----------------------------------------------------------------
            # FILTRE 2 : Type d'événement = EnvironmentalObstruction
            # ----------------------------------------------------------------
            # Le type d'enregistrement est stocké dans un attribut xsi:type
            record_type_raw = record.get('{http://www.w3.org/2001/XMLSchema-instance}type', '')
            record_type = record_type_raw.replace('ns2:', '')
            
            # On ne garde que les obstructions environnementales
            if 'EnvironmentalObstruction' not in record_type_raw:
                continue
            
            stats['environmental_obstruction'] += 1
            
            # ----------------------------------------------------------------
            # FILTRE 3 : Sous-type = flooding ou flashFloods
            # ----------------------------------------------------------------
            # Le sous-type précise la nature de l'obstruction
            env_type_elem = record.find('.//ns2:environmentalObstructionType', NS)
            env_subtype = None
            
            if env_type_elem is not None:
                env_subtype = env_type_elem.text
                
                # On ne garde que les inondations (flooding) ou crues soudaines (flashFloods)
                if env_subtype not in ['flooding', 'flashFloods']:
                    continue
            else:
                # Fallback : si le sous-type n'est pas explicitement défini,
                # on cherche des mots-clés d'inondation dans le contenu XML
                record_text = etree.tostring(record, encoding='unicode').lower()
                if not any(kw in record_text for kw in ['inond', 'crue', 'flood']):
                    continue
                
                # Marquage spécial pour indiquer une détection par mots-clés
                env_subtype = 'flooding-detected-by-keywords'
            
            # Si on arrive ici, l'événement est une inondation
            stats['inondations'] += 1
            stats['par_subtype'][env_subtype] = stats['par_subtype'].get(env_subtype, 0) + 1
            
            # ----------------------------------------------------------------
            # EXTRACTION DES DATES ET CALCUL DU STATUT
            # ----------------------------------------------------------------
            
            # Date de début de l'événement (obligatoire)
            start_elem = record.find('.//ns2:overallStartTime', NS)
            if start_elem is None:
                continue  # Pas de date de début = événement invalide
            
            # Conversion de la date ISO en objet datetime
            # Suppression des fuseaux horaires pour simplifier les comparaisons
            try:
                start_date = datetime.fromisoformat(start_elem.text.replace('+02:00', '').replace('+01:00', ''))
            except:
                continue  # Date mal formée = événement ignoré
            
            # Date de fin de l'événement (optionnelle)
            end_elem = record.find('.//ns2:overallEndTime', NS)
            is_active = True  # Par défaut, l'événement est considéré en cours
            end_date_iso = None
            
            if end_elem is not None:
                end_date_iso = end_elem.text
                try:
                    end_date = datetime.fromisoformat(end_elem.text.replace('+02:00', '').replace('+01:00', ''))
                    # L'événement est actif si la date de fin n'est pas encore passée
                    is_active = now <= end_date
                except:
                    pass  # Si la date de fin est mal formée, on reste sur is_active=True
            
            # Mise à jour des compteurs d'événements actifs/terminés
            if is_active:
                stats['actives'] += 1
            else:
                stats['terminees'] += 1
            
            # ----------------------------------------------------------------
            # EXTRACTION DES COORDONNÉES GPS
            # ----------------------------------------------------------------
            # Les coordonnées sont essentielles pour le format GeoJSON
            
            lat_elems = record.findall('.//ns2:latitude', NS)
            lon_elems = record.findall('.//ns2:longitude', NS)
            
            # Vérification de la présence des coordonnées
            if not lat_elems or not lon_elems:
                stats['sans_coords'] += 1
                continue  # Pas de coordonnées = événement non localisable, on l'ignore
            
            # Conversion des coordonnées en float
            try:
                lat = float(lat_elems[0].text)
                lon = float(lon_elems[0].text)
            except (ValueError, AttributeError):
                stats['sans_coords'] += 1
                continue  # Coordonnées invalides
            
            # ----------------------------------------------------------------
            # EXTRACTION DES INFORMATIONS COMPLÉMENTAIRES
            # ----------------------------------------------------------------
            
            # Numéro de route concernée (ex: N165, D123, etc.)
            road_elem = record.find('.//ns2:roadNumber', NS)
            road = road_elem.text if road_elem is not None else 'N/A'
            
            # Extraction de toutes les descriptions/commentaires en français
            comments = []
            for comment_elem in record.findall('.//ns2:generalPublicComment/ns2:comment/ns2:values/ns2:value[@lang="fr"]', NS):
                if comment_elem.text:
                    comments.append(comment_elem.text)
            
            # Concaténation des commentaires avec un séparateur
            description = ' | '.join(comments) if comments else 'Pas de description'
            
            # Mise à jour des statistiques de sévérité
            stats['par_severite'][severity] = stats['par_severite'].get(severity, 0) + 1
            
            # ----------------------------------------------------------------
            # CRÉATION DE LA FEATURE GEOJSON
            # ----------------------------------------------------------------
            # Structure conforme à la spécification GeoJSON (RFC 7946)
            
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat]  # Format GeoJSON : [longitude, latitude]
                },
                "properties": {
                    "id": sit_id,                          # Identifiant unique
                    "source": source,                       # Source de données (DIR Ouest/DIRO)
                    "road": road,                           # Route concernée
                    "type": record_type,                    # Type d'événement
                    "subtype": env_subtype,                 # Sous-type (flooding/flashFloods)
                    "problem": "Inondation",                # Nature du problème
                    "severity": severity,                   # Niveau de sévérité
                    "description": description[:300],       # Description limitée à 300 caractères
                    "start_date": start_elem.text,          # Date de début (format ISO)
                    "end_date": end_date_iso,               # Date de fin (peut être None)
                    "is_active": is_active,                 # Boolean : événement actif ?
                    "status": "en_cours" if is_active else "terminee",  # Statut lisible
                    "updated_at": datetime.now().isoformat()  # Horodatage de la mise à jour
                }
            }
            
            # Ajout de la feature à la liste
            features.append(feature)
    
    # Affichage du résumé du parsing
    print(f"Extraction terminée : {stats['inondations']} inondations DIR Ouest "
          f"({stats['actives']} actives, {stats['terminees']} terminées)")
    
    return features, stats


# ============================================================================
# FONCTIONS D'EXPORT
# ============================================================================

def create_geojson(features, stats):
    """
    Crée et sauvegarde le fichier GeoJSON ainsi que le fichier de statistiques.
    
    Cette fonction génère deux fichiers :
    1. Un GeoJSON avec toutes les inondations (format exploitable par les SIG)
    2. Un fichier texte avec des statistiques détaillées (pour le reporting)
    
    Args:
        features (list): Liste des features GeoJSON à exporter
        stats (dict): Dictionnaire contenant les statistiques de parsing
        
    Returns:
        None
        
    Side effects:
        - Crée le répertoire 'data/' s'il n'existe pas
        - Écrit les fichiers OUTPUT_FILE et STATS_FILE
    """
    
    # Construction de la structure GeoJSON complète
    # Conforme à la spécification RFC 7946
    geojson = {
        "type": "FeatureCollection",
        "metadata": {
            "generated_at": datetime.now().isoformat(),  # Date de génération
            "source": "DATEX II - Bison Futé",            # Source des données
            "filter": "Inondations DIR Ouest (Bretagne / Pays de la Loire)",  # Filtre appliqué
            "count": len(features),                       # Nombre total d'inondations
            "count_active": stats['actives'],             # Nombre d'inondations actives
            "count_finished": stats['terminees'],         # Nombre d'inondations terminées
            "statistics": stats                           # Statistiques complètes
        },
        "features": features  # Liste des features GeoJSON
    }
    
    # Création du répertoire de sortie si nécessaire
    os.makedirs('data', exist_ok=True)
    
    # ----------------------------------------------------------------
    # EXPORT DU GEOJSON
    # ----------------------------------------------------------------
    # Sauvegarde avec indentation pour lisibilité et UTF-8 pour les accents
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, indent=2, ensure_ascii=False)
    
    print(f"Fichier GeoJSON sauvegardé : {OUTPUT_FILE}")
    
    # ----------------------------------------------------------------
    # EXPORT DU FICHIER DE STATISTIQUES
    # ----------------------------------------------------------------
    # Génération d'un rapport texte lisible pour le suivi et le monitoring
    with open(STATS_FILE, 'w', encoding='utf-8') as f:
        # En-tête du rapport
        f.write(f"STATISTIQUES INONDATIONS DIR OUEST\n")
        f.write(f"=" * 50 + "\n\n")
        
        # Informations générales
        f.write(f"Généré le : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Zone géographique : DIR Ouest (Bretagne / Pays de la Loire)\n")
        f.write(f"Filtres appliqués : EnvironmentalObstruction + flooding/flashFloods\n\n")
        
        # Statistiques de filtrage
        f.write(f"Situations totales dans le flux : {stats['total_situations']}\n")
        f.write(f"Situations DIR Ouest : {stats['dir_ouest']}\n")
        f.write(f"Type EnvironmentalObstruction : {stats['environmental_obstruction']}\n")
        f.write(f"INONDATIONS IDENTIFIÉES : {stats['inondations']}\n")
        f.write(f"  |-- En cours : {stats['actives']}\n")
        f.write(f"  |-- Terminées : {stats['terminees']}\n")
        f.write(f"Événements sans coordonnées GPS : {stats['sans_coords']}\n\n")
        
        # Répartition par sévérité
        if stats['par_severite']:
            f.write("Répartition par niveau de sévérité :\n")
            for sev, count in sorted(stats['par_severite'].items()):
                f.write(f"  - {sev}: {count}\n")
        
        # Répartition par sous-type
        if stats['par_subtype']:
            f.write("\nRépartition par sous-type d'inondation :\n")
            for typ, count in sorted(stats['par_subtype'].items()):
                f.write(f"  - {typ}: {count}\n")
    
    print(f"Statistiques sauvegardées : {STATS_FILE}")


# ============================================================================
# FONCTION PRINCIPALE
# ============================================================================

def main():
    """
    Point d'entrée principal du script.
    
    Cette fonction orchestre l'ensemble du processus :
    1. Récupération du flux XML DATEX II
    2. Parsing et filtrage des inondations DIR Ouest
    3. Export au format GeoJSON et génération des statistiques
    
    En cas d'erreur, affiche un message et propage l'exception.
    """
    print("=" * 60)
    print("RÉCUPÉRATION INONDATIONS DATEX II - DIR OUEST")
    print("=" * 60)
    
    try:
        # Étape 1 : Récupération du flux XML depuis l'API
        xml_content = fetch_xml()
        
        # Étape 2 : Parsing du XML et application des filtres
        features, stats = parse_datex(xml_content)
        
        # Étape 3 : Export des résultats
        create_geojson(features, stats)
        
        # Message de succès
        print("\n" + "=" * 60)
        print("SUCCÈS - Données inondations mises à jour")
        print("=" * 60)
        
    except Exception as e:
        # Gestion des erreurs avec affichage détaillé
        print("\n" + "=" * 60)
        print(f"ERREUR CRITIQUE : {e}")
        print("=" * 60)
        raise  # Propagation de l'exception pour le debugging


# ============================================================================
# POINT D'ENTRÉE DU SCRIPT
# ============================================================================

if __name__ == '__main__':
    # Exécution de la fonction principale uniquement si le script est lancé directement
    # (pas lors d'un import en tant que module)
    main()
