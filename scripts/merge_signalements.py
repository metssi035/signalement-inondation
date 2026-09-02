#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de fusion des flux inforoute (portage Python de grist-to-geojson.js).

Fusionne les données d'inondations de 7 sources différentes en un seul GeoJSON,
avec archivage annuel et monitoring de l'état de chaque flux.

Sources de données :
  1. Grist 35 : signalements manuels saisis par les agents
  2. CD44 : département de Loire-Atlantique (API REST)
  3. Rennes Métropole : service WFS (retourne du GeoJSON, pas du XML)
  4. CD35 : département d'Ille-et-Vilaine (API OGC, signalements ponctuels)
  5. CD35 Linéaire : département d'Ille-et-Vilaine (API OGC, tronçons linéaires)
  6. CD56 : département du Morbihan (API OGC)
  7. DIRO : DIR Ouest (fichier GeoJSON généré par fetch_datex_diro.py)

Fichiers générés :
  - signalements.geojson : tous les signalements actifs fusionnés
  - signalements_inondation.xml : export Datex II des coupures pour cause d'inondation
  - signalements_inondation.csv : export CSV grand public (toutes causes)
  - metadata.json : statistiques + monitoring des flux
  - archives/signalements_YYYY.geojson : archives par année
  - archives/last_run.json : état de la dernière exécution
"""

import collections
import concurrent.futures
import csv
import hashlib
import itertools
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests
import pyproj

# ================================================================================
# CONFIGURATION
# ================================================================================

GRIST_DOC_ID = os.environ.get("GRIST_DOC_ID")
GRIST_API_KEY = os.environ.get("GRIST_API_KEY")
TABLE_ID = "Signalements"

# Coupe-circuit par source : variable CI/CD DISABLED_SOURCES, liste de clés
# séparées par des virgules (ex. "cd35_inondations,cd35_lineaire"). Une source
# désactivée n'est plus interrogée du tout et disparaît des exports, sans
# toucher à son historique déjà archivé.
KNOWN_SOURCES = {
    "grist_35", "cd44", "rennes_metropole",
    "cd35_inondations", "cd35_lineaire", "cd56", "diro",
}
DISABLED_SOURCES = {s.strip() for s in os.environ.get("DISABLED_SOURCES", "").split(",") if s.strip()}
for _unknown_source in DISABLED_SOURCES - KNOWN_SOURCES:
    print(f"ATTENTION: DISABLED_SOURCES contient '{_unknown_source}', qui ne correspond à aucune source connue ({', '.join(sorted(KNOWN_SOURCES))})")

DIRO_FILE_PATH = "data/inondations-diro.geojson"

CD35_OGC_BASE = "https://services1.arcgis.com/jGLANYlFVVx3nuxa/ArcGIS/rest/services/Inondation_vue3/OGCFeatureServer"
CD35_LINEAIRE_OGC_BASE = "https://services1.arcgis.com/jGLANYlFVVx3nuxa/ArcGIS/rest/services/Inondation_lineaire_vue3/OGCFeatureServer"
CD56_OGC_BASE = "https://services.arcgis.com/4GFMPbPboxIs6KOG/arcgis/rest/services/INONDATION/OGCFeatureServer"
RENNES_METRO_WFS_URL = (
    "https://public.sig.rennesmetropole.fr/geoserver/ows?SERVICE=WFS&REQUEST=GetFeature"
    "&VERSION=2.0.0&TYPENAMES=trp_rout:routes_coupees&OUTPUTFORMAT=json"
)

HTTP_TIMEOUT = 30

# ================================================================================
# IDENTIFIANTS UNIQUES
# ================================================================================

_unique_id_counter = itertools.count(1)


def generate_unique_id():
    return next(_unique_id_counter)


# ================================================================================
# DATES ET HEURES (fuseau Europe/Paris)
# ================================================================================


def _iso_now():
    """Équivalent de Date.toISOString() en JS : UTC, millisecondes, suffixe Z."""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def get_datetime_fr():
    now = datetime.now(timezone.utc)
    now_paris = now.astimezone(ZoneInfo("Europe/Paris"))
    return {
        "iso": _iso_now(),
        "local": now_paris.strftime("%d/%m/%Y à %H:%M"),
        "timezone": "Europe/Paris",
    }


def _parse_datetime_value(date_value):
    if isinstance(date_value, str):
        text = date_value.strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if isinstance(date_value, (int, float)):
        # Grist/ArcGIS renvoient des timestamps en secondes ou en millisecondes
        ms = date_value if date_value > 100_000_000_000 else date_value * 1000
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return None


def format_date(date_value):
    """Convertit une date (ISO string ou timestamp) en 'DD/MM/YYYY à HHhMM' (Europe/Paris)."""
    if not date_value:
        return ""
    try:
        dt = _parse_datetime_value(date_value)
        if dt is None:
            return ""
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt_paris = dt.astimezone(ZoneInfo("Europe/Paris"))
        return dt_paris.strftime("%d/%m/%Y à %Hh%M")
    except Exception:
        return ""


def is_older_than_3_days(date_string):
    """Reproduit fidèlement le JS : compare en heure système naïve (comme new Date()),
    pas en Europe/Paris explicite — même comportement que grist-to-geojson.js."""
    if not date_string:
        return False
    try:
        match = re.match(r"(\d{2})/(\d{2})/(\d{4})\s+à\s+(\d{2})h(\d{2})", date_string)
        if not match:
            return False
        day, month, year, hours, minutes = (int(g) for g in match.groups())
        date_obj = datetime(year, month, day, hours, minutes)
        diff_days = (datetime.now() - date_obj).total_seconds() / 86400
        return diff_days > 3
    except Exception:
        return False


def get_year_from_date_debut(date_string):
    if not date_string:
        return None
    match = re.search(r"\d{2}/\d{2}/(\d{4})", date_string)
    return int(match.group(1)) if match else None


# ================================================================================
# PROJECTIONS CARTOGRAPHIQUES (Lambert 93 / CC48 → WGS84)
# ================================================================================

_lambert93_to_wgs84 = pyproj.Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)
_cc48_to_wgs84 = pyproj.Transformer.from_crs("EPSG:3948", "EPSG:4326", always_xy=True)


def convert_lambert93_to_wgs84(x, y):
    return _lambert93_to_wgs84.transform(x, y)


def convert_cc48_to_wgs84(x, y):
    return _cc48_to_wgs84.transform(x, y)


# ================================================================================
# FILTRAGE DES SIGNALEMENTS RÉSOLUS DEPUIS PLUS DE 3 JOURS
# ================================================================================

KeepResult = collections.namedtuple("KeepResult", ["keep", "filtered_resolved"])


def should_keep_feature(feature):
    props = feature["properties"]
    if props.get("statut_actif") is True:
        return KeepResult(True, False)
    if props.get("statut_resolu") is True:
        if props.get("date_fin") and is_older_than_3_days(props["date_fin"]):
            return KeepResult(False, True)
        return KeepResult(True, False)
    return KeepResult(True, False)


# ================================================================================
# ARCHIVAGE ANNUEL
# ================================================================================


def load_archive(year):
    archive_dir = "archives"
    archive_path = os.path.join(archive_dir, f"signalements_{year}.geojson")
    os.makedirs(archive_dir, exist_ok=True)
    if os.path.exists(archive_path):
        try:
            with open(archive_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as exc:
            print(f"Erreur lecture archive {year}, création nouvelle: {exc}")
    return {
        "type": "FeatureCollection",
        "features": [],
        "metadata": {"year": year, "created": _iso_now(), "last_update": _iso_now()},
    }


def save_archive(year, geojson):
    archive_path = os.path.join("archives", f"signalements_{year}.geojson")
    geojson["metadata"]["last_update"] = _iso_now()
    with open(archive_path, "w", encoding="utf-8") as f:
        json.dump(geojson, f, indent=2, ensure_ascii=False)


def load_last_run():
    path = "archives/last_run.json"
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as exc:
            print(f"Erreur lecture last_run.json: {exc}")
            return {"date": None, "actifs": {}}
    return {"date": None, "actifs": {}}


def save_last_run(data):
    with open("archives/last_run.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def find_in_archive(archive, id_source, source):
    """Index de la DERNIÈRE entrée correspondant à (id_source, source), pas
    la première. Une réutilisation d'ID crée plusieurs entrées archivées
    pour le même (id_source, source) (cf add_or_update_in_archive) - il faut
    toujours comparer/mettre à jour contre l'épisode le plus récent. En
    renvoyant la première, une comparaison de date_debut contre une entrée
    déjà périmée redéclenchait "ID réutilisé" à chaque run tant que le
    signalement restait dans le flux, créant un nouveau doublon à chaque
    passage - bug constaté en production (74 doublons pour un même
    signalement Grist, cf logs CI du 20/08/2026)."""
    if not archive or "features" not in archive:
        return -1
    last_match = -1
    for i, f in enumerate(archive["features"]):
        if f["properties"].get("id_source") == id_source and f["properties"].get("source") == source:
            last_match = i
    return last_match


def add_or_update_in_archive(feature):
    props = feature["properties"]
    year = get_year_from_date_debut(props.get("date_debut"))
    if not year:
        print(f"Pas d'année pour {props.get('source')} - {props.get('id_source')}")
        return

    archive = load_archive(year)
    existing_index = find_in_archive(archive, props.get("id_source"), props.get("source"))

    if existing_index >= 0:
        existing = archive["features"][existing_index]
        existing_props = existing["properties"]

        # Une date_debut différente ne signale un ID réutilisé (nouvel
        # événement) que si l'épisode existant n'est plus actif - une vraie
        # réutilisation d'ID ne peut survenir qu'après la fermeture du
        # précédent événement. Sinon (épisode encore actif), c'est une
        # dérive de la date source, pas un nouvel événement : constaté sur
        # CD56, dont date_constatation (source de date_debut, cf
        # cd56_to_feature()) peut changer d'un relevé à l'autre pour une
        # même coupure continue - sans cette garde, chaque relevé aurait
        # fragmenté l'archive en plusieurs épisodes pour la même coupure.
        #
        # Testé sur statut_actif, pas statut_resolu : CD44/CD56 codent
        # statut_resolu en dur à False (ils retirent juste l'entrée de leur
        # flux, geré par detect_deleted_signalements() plus bas qui marque
        # "Supprimé" sans jamais toucher statut_resolu) - statut_resolu ne
        # deviendrait donc jamais vrai pour ces 2 sources, désactivant la
        # détection de réutilisation. CD35 peut désormais coder statut_resolu
        # à True (via Date_ouverture, cf _cd35_properties()), mais seulement
        # quand la source le dit explicitement - même repli nécessaire tant
        # qu'elle ne le fait pas. statut_actif, lui, est mis à jour à la fois
        # par la résolution explicite (juste en dessous) et par la détection
        # de suppression.
        #
        # "or route différente" (02/09) : Grist réattribue l'id d'une ligne
        # supprimée à la ligne suivante créée (constaté en test). Si la
        # suppression et la recréation arrivent entre deux runs consécutifs,
        # rien n'a jamais vu l'ancien épisode "disparaître" (detect_deleted_
        # signalements() plus bas ne le remarque que si un run a observé la
        # ligne encore active, puis absente) - sans ce 2e critère, statut_
        # actif restait à True en continu et l'ancien épisode se voyait juste
        # attribuer la nouvelle géométrie/type_coupure/commentaire en
        # écrasant l'ancien, sans jamais recréer de ligne (route/commune/
        # date_debut de l'archive restaient ceux de l'épisode supprimé).
        id_reutilise = existing_props.get("date_debut") != props.get("date_debut") and (
            not existing_props.get("statut_actif")
            or existing_props.get("route") != props.get("route")
        )

        if id_reutilise:
            archive_feature = {**feature, "properties": {**props, "date_suppression": ""}}
            archive["features"].append(archive_feature)
            print(f"   ➕ Nouvel signalement {props.get('source')} {props.get('id_source')} (ID réutilisé) dans archive {year}")
        else:
            if not existing_props.get("statut_resolu") and props.get("statut_resolu") and props.get("date_fin"):
                existing_props["statut"] = "Résolu"
                existing_props["statut_resolu"] = True
                existing_props["statut_actif"] = False
                existing_props["date_fin"] = props["date_fin"]
                print(f"   ✏️ Mise à jour {props.get('source')} {props.get('id_source')}: Actif → Résolu")
            existing["geometry"] = feature["geometry"]
            existing_props["type_coupure"] = props.get("type_coupure")
            existing_props["commentaire"] = props.get("commentaire")
    else:
        archive_feature = {**feature, "properties": {**props, "date_suppression": ""}}
        archive["features"].append(archive_feature)
        print(f"   ➕ Ajout {props.get('source')} {props.get('id_source')} dans archive {year}")

    save_archive(year, archive)


def detect_deleted_signalements(current_features):
    """Détecte les signalements dont l'archive les dit encore actifs mais qui
    ont disparu du flux courant sans jamais être passés par un statut_resolu
    explicite. Pour les sources qui fournissent bien un date_fin (ex: Rennes
    Métropole, et CD35 depuis fin 08/2026 quand Date_ouverture est renseigné
    - cf _cd35_properties()), l'archive est déjà à jour via
    add_or_update_in_archive() avant même que cette fonction ne s'exécute -
    ici on ne fait que boucher le cas où la source ne dit rien avant de
    retirer l'entrée (ex: CD35 quand Date_ouverture reste vide, comportement
    observé jusqu'ici sur tous les cas), avec date_suppression (l'heure où
    NOUS l'avons remarqué) plutôt qu'une date_fin qu'on n'a jamais reçue."""
    last_run = load_last_run()
    now = datetime.now()
    date_suppression = now.strftime("%d/%m/%Y à %Hh%M")

    current_actifs = {
        "Saisie Grist": [], "CD44": [], "Rennes Métropole": [],
        "CD35 Inondations": [], "CD35 Inondations Linéaire": [], "CD56": [], "DIRO": [],
    }
    for feature in current_features:
        props = feature["properties"]
        if props.get("statut_actif") and props.get("id_source") and props.get("source") in current_actifs:
            current_actifs[props["source"]].append(props["id_source"])

    if not last_run.get("date"):
        print("Première exécution - initialisation de last_run.json")
        save_last_run({"date": _iso_now(), "actifs": current_actifs})
        return

    deleted_count = 0
    for source, previous_ids in (last_run.get("actifs") or {}).items():
        current_ids = current_actifs.get(source, [])
        for id_source in previous_ids:
            if id_source in current_ids:
                continue
            current_year = now.year
            for year in (current_year, current_year - 1):
                archive = load_archive(year)
                index = find_in_archive(archive, id_source, source)
                if index < 0:
                    continue
                feature = archive["features"][index]
                if feature["properties"].get("statut_actif") and not feature["properties"].get("date_suppression"):
                    feature["properties"]["statut"] = "Supprimé"
                    feature["properties"]["statut_actif"] = False
                    feature["properties"]["date_suppression"] = date_suppression
                    save_archive(year, archive)
                    print(f"Suppression détectée: {source} {id_source} (archive {year})")
                    deleted_count += 1
                break

    if deleted_count > 0:
        print(f"Total suppressions détectées: {deleted_count}")

    save_last_run({"date": _iso_now(), "actifs": current_actifs})


# ================================================================================
# MONITORING DES FLUX
# ================================================================================

flux_monitor = {
    "grist_35": None, "cd44": None, "rennes_metropole": None,
    "cd35_inondations": None, "cd35_lineaire": None, "cd56": None, "diro": None,
}


def monitor_fetch(source_name, fetch_fn):
    start = time.monotonic()
    status = {
        "source": source_name, "status": "ERROR", "records": 0,
        "responseTime": 0, "lastError": None, "lastSuccess": None, "message": None,
    }
    try:
        data = fetch_fn()
        status["responseTime"] = int((time.monotonic() - start) * 1000)

        if isinstance(data, dict) and "features" in data:
            status["records"] = len(data.get("features") or [])
        else:
            status["records"] = len(data) if data else 0

        if status["records"] == 0:
            status["status"] = "EMPTY"
            status["message"] = "API accessible mais 0 résultats"
        else:
            status["status"] = "OK"
            status["message"] = f"{status['records']} signalement(s) récupéré(s)"
            status["lastSuccess"] = get_datetime_fr()["local"]

        flux_monitor[source_name] = status
        return data
    except Exception as exc:
        status["responseTime"] = int((time.monotonic() - start) * 1000)
        status["status"] = "ERROR"
        status["lastError"] = str(exc)
        status["message"] = f"Erreur: {exc}"
        flux_monitor[source_name] = status
        return []


# ================================================================================
# RENNES MÉTROPOLE - WFS ROUTES COUPÉES
# ================================================================================


def fetch_rennes_metro_data():
    try:
        print("[Rennes Métropole] Récupération via WFS...")
        resp = requests.get(RENNES_METRO_WFS_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=HTTP_TIMEOUT)
        if not resp.ok:
            print(f"[Rennes Métropole] HTTP {resp.status_code}")
            return {"features": [], "needsConversion": False}

        geojson = resp.json()
        raw_features = geojson.get("features") or []
        print(f"   GeoJSON reçu: {len(raw_features)} features")

        filtered_features = [
            f for f in raw_features
            if ((f.get("properties") or {}).get("raison") or "").lower() == "inondation"
        ]
        print(f"   Filtrés (raison=inondation): {len(filtered_features)} features")

        # CC48 (EPSG:3948) : X entre 1300000-2100000 / WGS84 : longitude entre -180 et 180
        needs_conversion = False
        if filtered_features:
            first_geom = filtered_features[0].get("geometry") or {}
            geom_type = first_geom.get("type")
            coords = first_geom.get("coordinates")
            test_coord = None
            if geom_type == "Point":
                test_coord = coords[0]
            elif geom_type == "LineString":
                test_coord = coords[0][0]
            elif geom_type == "MultiLineString":
                test_coord = coords[0][0][0]

            if test_coord is not None and abs(test_coord) > 1000:
                needs_conversion = True
                print(f"   Coordonnées détectées en projection CC48 (EPSG:3948): X={test_coord}")
            elif test_coord is not None:
                print(f"   Coordonnées déjà en WGS84: X={test_coord}")

        print(f"[Rennes Métropole] {len(filtered_features)} features filtrées avec succès")
        return {"features": filtered_features, "needsConversion": needs_conversion}
    except requests.RequestException as exc:
        print(f"[Rennes Métropole] {exc}")
        return {"features": [], "needsConversion": False}


def rennes_metro_to_feature(feature, needs_conversion=False):
    try:
        geometry = feature.get("geometry")
        if not geometry:
            return None

        if needs_conversion:
            geom_type = geometry.get("type")
            if geom_type == "Point":
                x, y = geometry["coordinates"]
                lon, lat = convert_cc48_to_wgs84(x, y)
                geometry = {"type": "Point", "coordinates": [lon, lat]}
            elif geom_type == "LineString":
                geometry = {
                    "type": "LineString",
                    "coordinates": [list(convert_cc48_to_wgs84(x, y)) for x, y in geometry["coordinates"]],
                }
            elif geom_type == "MultiLineString":
                geometry = {
                    "type": "MultiLineString",
                    "coordinates": [
                        [list(convert_cc48_to_wgs84(x, y)) for x, y in line]
                        for line in geometry["coordinates"]
                    ],
                }

        props = feature.get("properties") or {}
        etat = (props.get("etat") or "").lower()
        is_resolu = etat in ("terminé", "termine")
        is_actif = etat == "en cours"
        statut = "Résolu" if is_resolu else ("Actif" if is_actif else etat)
        id_source = props.get("id") or props.get("gid")

        return {
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                "id": generate_unique_id(),
                "id_source": id_source,
                "source": "Rennes Métropole",
                "route": props.get("toponyme") or "",
                "commune": props.get("comm_nom") or "",
                "cause": "Inondation",
                "statut": statut,
                "statut_actif": is_actif,
                "statut_resolu": is_resolu,
                "type_coupure": "Totale",
                "sens_circulation": "",
                "commentaire": props.get("commentaires") or "",
                "date_debut": format_date(props.get("date_debut")),
                "date_fin": format_date(props.get("date_fin")),
                "date_saisie": format_date(props.get("date_debut")),
                "date_suppression": "",
                "gestionnaire": "Rennes Métropole",
            },
        }
    except Exception as exc:
        print(f"Erreur conversion Rennes Métropole: {exc}")
        return None


# ================================================================================
# CD35 / CD56 - API OGC FEATURE
# ================================================================================


def _fetch_ogc_features(base_url, label):
    try:
        print(f"[{label}] Récupération via OGC API REST...")
        collections_resp = requests.get(f"{base_url}/collections?f=json", headers={"User-Agent": "Mozilla/5.0"}, timeout=HTTP_TIMEOUT)
        if not collections_resp.ok:
            print(f"[{label}] HTTP {collections_resp.status_code} sur /collections")
            return []

        collections = collections_resp.json().get("collections") or []
        if not collections:
            print(f"[{label}] Aucune collection trouvée")
            return []

        collection_id = collections[0]["id"]
        items_resp = requests.get(
            f"{base_url}/collections/{collection_id}/items?f=json",
            headers={"User-Agent": "Mozilla/5.0"}, timeout=HTTP_TIMEOUT,
        )
        if not items_resp.ok:
            print(f"[{label}] HTTP {items_resp.status_code} sur /items")
            return []

        features = items_resp.json().get("features") or []
        print(f"[{label}] {len(features)} features récupérées avec succès")
        return features
    except requests.RequestException as exc:
        print(f"[{label}] {exc}")
        return []


def fetch_cd35_inondations_data():
    return _fetch_ogc_features(CD35_OGC_BASE, "CD35 Inondations")


def fetch_cd35_lineaire_data():
    return _fetch_ogc_features(CD35_LINEAIRE_OGC_BASE, "CD35 Inondations Linéaire")


def fetch_cd56_data():
    return _fetch_ogc_features(CD56_OGC_BASE, "CD56")


def _cd35_properties(props, source):
    """Champs communs aux deux couches OGC CD35 (ponctuel + linéaire) : même
    schéma de champs source (route, etat_circulation, commune, agence, prd,
    prf, lieu_dit, Date_saisie en epoch ms). OBJECTID est en majuscules sur la
    couche linéaire, en minuscules (objectid) sur la couche ponctuelle
    (vérifié sur les deux flux réels) — même repli que cd56_to_feature().

    etat_circulation ne pilote que type_coupure (comparaison insensible à la
    casse) : seules "route coupée" et "route coupée partiellement" ont été
    observées à ce jour, et le champ est explicitement annoncé non stabilisé
    côté CD35 — donc pas de table de correspondance exhaustive.

    Date_ouverture (epoch ms, ajouté par CD35 fin 08/2026) donne enfin une
    date de réouverture explicite quand elle est renseignée : on bascule
    alors statut/statut_actif/statut_resolu comme une source résolue. Tant
    qu'elle est null (cas de tous les tronçons actifs observés jusqu'ici),
    on garde le comportement historique (Actif, date_fin vide) — CD35 peut
    toujours retirer l'entrée du flux sans jamais renseigner ce champ, le
    filet de sécurité detect_deleted_signalements() reste donc nécessaire."""
    etat = (props.get("etat_circulation") or "").lower()
    type_coupure = "Partielle" if "partielle" in etat else "Totale"
    date_value = format_date(props.get("Date_saisie"))
    date_fin = format_date(props.get("Date_ouverture"))
    is_resolu = bool(date_fin)
    return {
        "id": generate_unique_id(),
        "id_source": props.get("OBJECTID") or props.get("objectid"),
        "source": source,
        "route": props.get("route") or "",
        "commune": props.get("commune") or "",
        "cause": "Inondation",
        "statut": "Résolu" if is_resolu else "Actif",
        "statut_actif": not is_resolu,
        "statut_resolu": is_resolu,
        "type_coupure": type_coupure,
        "sens_circulation": "",
        "commentaire": props.get("lieu_dit") or "",
        "date_debut": date_value,
        "date_fin": date_fin,
        "date_saisie": date_value,
        "date_suppression": "",
        "gestionnaire": "CD35",
        "agence": props.get("agence") or "",
        "pr_debut": props.get("prd") or "",
        "pr_fin": props.get("prf") or "",
    }


def cd35_inondations_to_feature(feature):
    try:
        geometry = feature.get("geometry")
        if not geometry:
            return None
        props = feature.get("properties") or {}
        return {
            "type": "Feature",
            "geometry": geometry,
            "properties": _cd35_properties(props, "CD35 Inondations"),
        }
    except Exception as exc:
        print(f"Erreur conversion CD35 Inondations: {exc}")
        return None


def cd35_lineaire_to_feature(feature):
    try:
        geometry = feature.get("geometry")
        if not geometry:
            return None
        props = feature.get("properties") or {}
        return {
            "type": "Feature",
            "geometry": geometry,
            "properties": _cd35_properties(props, "CD35 Inondations Linéaire"),
        }
    except Exception as exc:
        print(f"Erreur conversion CD35 Inondations Linéaire: {exc}")
        return None


def cd56_to_feature(feature):
    try:
        geometry = feature.get("geometry")
        if not geometry:
            return None
        props = feature.get("properties") or {}

        conditions = props.get("conditions_circulation") or props.get("conditionsCirculation") or ""
        if conditions.upper() not in ("COUPÉE", "INONDÉE PARTIELLE"):
            return None

        type_coupure = "Partielle" if conditions.upper() == "INONDÉE PARTIELLE" else "Totale"

        lineaire_inonde = props.get("lineaire_inonde") or props.get("lineaireInonde") or ""
        lineaire_text = (
            f"Longueur linéaire inondée : {lineaire_inonde}"
            if lineaire_inonde and lineaire_inonde not in ("0", "?")
            else ""
        )

        if conditions.upper() == "INONDÉE PARTIELLE":
            commentaire = "Inondation partielle"
            if lineaire_text:
                commentaire += f". {lineaire_text}"
        else:
            commentaire = lineaire_text

        return {
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                "id": generate_unique_id(),
                "id_source": props.get("OBJECTID") or props.get("objectid"),
                "source": "CD56",
                "route": props.get("rd") or "",
                "commune": props.get("commune") or "",
                "cause": "Inondation",
                "statut": "Actif",
                "statut_actif": True,
                "statut_resolu": False,
                "type_coupure": type_coupure,
                "sens_circulation": "",
                "commentaire": commentaire,
                "date_debut": format_date(props.get("date_constatation") or props.get("dateConstatation")),
                "date_fin": format_date(
                    props.get("Date_fin_d_évènement") or props.get("date_fin_evenement") or props.get("dateFin")
                ),
                "date_saisie": format_date(props.get("date_constatation") or props.get("dateConstatation")),
                "date_suppression": "",
                "gestionnaire": "CD56",
            },
        }
    except Exception as exc:
        print(f"Erreur conversion CD56: {exc}")
        return None


# ================================================================================
# GRIST 35
# ================================================================================


def fetch_grist_data():
    if not GRIST_DOC_ID or not GRIST_API_KEY:
        print("Grist credentials manquants")
        return []
    try:
        print("[Grist 35] Récupération...")
        url = f"https://grist.dataregion.fr/o/inforoute/api/docs/{GRIST_DOC_ID}/tables/{TABLE_ID}/records"
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {GRIST_API_KEY}", "Content-Type": "application/json"},
            timeout=HTTP_TIMEOUT,
        )
        if resp.status_code == 200:
            records = resp.json().get("records") or []
            print(f"[Grist 35] {len(records)} records")
            return records
        print(f"[Grist 35] HTTP {resp.status_code}")
        return []
    except requests.RequestException as exc:
        print(f"[Grist 35] {exc}")
        return []


def grist_to_feature(record):
    try:
        fields = record.get("fields") or {}

        if fields.get("geojson"):
            geometry = json.loads(fields["geojson"])
        elif fields.get("Latitude") and fields.get("Longitude"):
            geometry = {"type": "Point", "coordinates": [fields["Longitude"], fields["Latitude"]]}
        else:
            return None

        cause_field = fields.get("Cause")
        if isinstance(cause_field, list):
            cause = ", ".join(c for c in cause_field if c != "L")
        else:
            cause = cause_field or ""

        statut = fields.get("Statut") or "Actif"

        return {
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                "id": generate_unique_id(),
                "id_source": record.get("id"),
                "source": "Saisie Grist",
                "route": fields.get("Route") or "",
                "commune": fields.get("Commune") or "",
                "cause": cause or "Inondation",
                "statut": statut,
                "statut_actif": statut == "Actif",
                "statut_resolu": statut == "Résolu",
                "type_coupure": fields.get("Type_coupure") or "Totale",
                "sens_circulation": fields.get("Sens_circulation") or "",
                "commentaire": fields.get("Description") or "",
                "date_debut": format_date(fields.get("Date_heure_debut")),
                "date_fin": format_date(fields.get("Date_heure_fin")),
                "date_saisie": format_date(fields.get("Date_heure")),
                "date_suppression": "",
                "gestionnaire": fields.get("Gestionnaire") or "",
            },
        }
    except Exception:
        return None


# ================================================================================
# CD44
# ================================================================================


def fetch_cd44_data():
    try:
        print("[CD44] Récupération...")
        url = (
            "https://data.loire-atlantique.fr/api/explore/v2.1/catalog/datasets/"
            "224400028_info-route-departementale/records?limit=100"
        )
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=HTTP_TIMEOUT)
        if resp.status_code == 200:
            records = resp.json().get("results") or []
            print(f"[CD44] {len(records)} records")
            return records
        print(f"[CD44] HTTP {resp.status_code}")
        return []
    except requests.RequestException as exc:
        print(f"[CD44] {exc}")
        return []


def parse_cd44_date_fin(ligne4):
    if not ligne4:
        return ""
    try:
        du_au = re.search(r"au\s+(\d{2})/(\d{2})/(\d{4})", ligne4)
        if du_au:
            day, month, year = du_au.groups()
            return f"{day}/{month}/{year} à 00h00"

        fin = re.search(r"(\d{2})/(\d{2})/(\d{4})\s+à\s+(\d{1,2})h(\d{2})", ligne4)
        if fin:
            day, month, year, hours, minutes = fin.groups()
            return f"{day}/{month}/{year} à {hours.zfill(2)}h{minutes}"

        return ""
    except Exception:
        return ""


def cd44_to_feature(item):
    try:
        if item.get("type") not in ("Inondation", "inondation"):
            return None

        geometry = {"type": "Point", "coordinates": [item.get("longitude"), item.get("latitude")]}

        ligne2 = item.get("ligne2")
        route = " / ".join(ligne2) if isinstance(ligne2, list) else (ligne2 or "Route")
        commune = item.get("ligne3") or ""

        commentaire = item.get("ligne1") or ""
        if item.get("ligne5"):
            commentaire += (" - " if commentaire else "") + item["ligne5"]

        # L'API CD44 ne fournit aucun identifiant stable (vérifié : ni recordid, ni
        # champ id, même avec select=*/include_links=true). On construit un identifiant
        # synthétique à partir de route + commune + période de fermeture (ligne4), pour
        # que deux coupures CD44 distinctes ne s'écrasent plus dans l'archive annuelle.
        id_source = hashlib.md5(f"{route}|{commune}|{item.get('ligne4') or ''}".encode("utf-8")).hexdigest()[:12]

        return {
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                "id": generate_unique_id(),
                "id_source": id_source,
                "source": "CD44",
                "route": route,
                "commune": commune,
                "cause": "Inondation",
                "statut": "Actif",
                "statut_actif": True,
                "statut_resolu": False,
                "type_coupure": "Totale",
                "sens_circulation": "",
                "commentaire": commentaire,
                "date_debut": format_date(item.get("datepublication")),
                "date_fin": parse_cd44_date_fin(item.get("ligne4")),
                "date_saisie": format_date(item.get("datepublication")),
                "date_suppression": "",
                "gestionnaire": "CD44",
            },
        }
    except Exception:
        return None


# ================================================================================
# DIRO - DIR OUEST (DATEX II, via fetch_datex_diro.py)
# ================================================================================

_SEVERITY_LABELS = {"low": "Faible", "medium": "Moyenne", "high": "Élevée", "veryHigh": "Très élevée"}


def fetch_diro_data():
    try:
        print(f"[DIRO] Lecture du fichier {DIRO_FILE_PATH}...")
        if not os.path.exists(DIRO_FILE_PATH):
            print(f"   Fichier DIRO non trouvé ({DIRO_FILE_PATH})")
            return []

        with open(DIRO_FILE_PATH, "r", encoding="utf-8") as f:
            geojson = json.load(f)

        features = geojson.get("features") or []
        print(f"   {len(features)} features trouvées")

        active_features = [f for f in features if (f.get("properties") or {}).get("is_active") is True]
        print(f"   {len(active_features)} inondations actives")
        print(f"[DIRO] {len(active_features)} inondations récupérées")
        return active_features
    except Exception as exc:
        print(f"[DIRO] {exc}")
        return []


def diro_to_feature(feature):
    try:
        geometry = feature.get("geometry")
        if not geometry:
            return None
        props = feature.get("properties") or {}

        is_actif = props.get("is_active") is True
        is_resolu = props.get("is_active") is False
        statut = "Actif" if is_actif else "Résolu"

        severity = props.get("severity")
        severity_text = _SEVERITY_LABELS.get(severity, severity or "")

        parts = [
            props.get("description") or "",
            f"Sévérité: {severity_text}" if severity_text else "",
            f"Type: {props.get('subtype')}" if props.get("subtype") else "",
        ]
        commentaire = " | ".join(p for p in parts if p)

        return {
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                "id": generate_unique_id(),
                "id_source": props.get("id"),
                "source": "DIRO",
                "route": props.get("road") or "",
                "commune": "",
                "cause": "Inondation",
                "statut": statut,
                "statut_actif": is_actif,
                "statut_resolu": is_resolu,
                "type_coupure": "Totale",
                "sens_circulation": "",
                "commentaire": commentaire,
                "date_debut": format_date(props.get("start_date")),
                "date_fin": format_date(props.get("end_date")),
                "date_saisie": format_date(props.get("start_date")),
                "date_suppression": "",
                "gestionnaire": "DIRO - DIR Ouest",
            },
        }
    except Exception as exc:
        print(f"Erreur conversion DIRO: {exc}")
        return None


# ================================================================================
# EXPORT DATEX II (mêmes signalements que signalements.geojson)
# ================================================================================
#
# Reproduit l'intégralité de "features" en Datex II, sans filtre — signalements.geojson
# ne contient déjà que des inondations en pratique. situationRecord de type
# EnvironmentalObstruction (sous-type flooding) : le même que celui lu par
# fetch_datex_diro.py depuis le flux DIR Ouest, seul type Datex II qu'on sache
# utilisé en pratique côté France pour ce cas d'usage.

DATEX_NS = "http://datex2.eu/schema/2/2_0"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"


def _representative_point(geometry):
    """Point représentatif d'une géométrie pour Datex II (PointByCoordinates).
    Pour une ligne, prend le point médian du tracé — même repli que
    getGeometryCenter() côté affichage dans featurestyles-signalements.js."""
    geom_type = geometry.get("type")
    coords = geometry.get("coordinates")
    if geom_type == "Point" and coords:
        return coords[0], coords[1]
    if geom_type == "LineString" and coords:
        mid = coords[len(coords) // 2]
        return mid[0], mid[1]
    if geom_type == "MultiLineString" and coords and coords[0]:
        line = coords[0]
        mid = line[len(line) // 2]
        return mid[0], mid[1]
    return None


def _fr_date_to_iso(date_string):
    """Convertit 'DD/MM/YYYY à HHhMM' (Europe/Paris) en ISO 8601 UTC."""
    if not date_string:
        return None
    match = re.match(r"(\d{2})/(\d{2})/(\d{4})\s+à\s+(\d{2})h(\d{2})", date_string)
    if not match:
        return None
    day, month, year, hours, minutes = (int(g) for g in match.groups())
    dt_paris = datetime(year, month, day, hours, minutes, tzinfo=ZoneInfo("Europe/Paris"))
    return dt_paris.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_datex2_export(features, output_path="signalements_inondation.xml"):
    ET.register_namespace("", DATEX_NS)
    ET.register_namespace("xsi", XSI_NS)

    def tag(name):
        return f"{{{DATEX_NS}}}{name}"

    root = ET.Element(tag("d2LogicalModel"), {"modelBaseVersion": "2"})

    exchange = ET.SubElement(root, tag("exchange"))
    supplier = ET.SubElement(exchange, tag("supplierIdentification"))
    ET.SubElement(supplier, tag("country")).text = "fr"
    ET.SubElement(supplier, tag("nationalIdentifier")).text = "DDTM35"

    payload = ET.SubElement(root, tag("payloadPublication"))
    payload.set(f"{{{XSI_NS}}}type", "SituationPublication")
    payload.set("lang", "fr")
    ET.SubElement(payload, tag("publicationTime")).text = _iso_now()
    creator = ET.SubElement(payload, tag("publicationCreator"))
    ET.SubElement(creator, tag("country")).text = "fr"
    ET.SubElement(creator, tag("nationalIdentifier")).text = "DDTM35"

    now_iso = _iso_now()

    for feature in features:
        props = feature["properties"]
        point = _representative_point(feature["geometry"])
        if point is None:
            continue

        situation = ET.SubElement(payload, tag("situation"), {"id": f"SIT-{props['id']}", "version": "1"})

        # headerInformation est obligatoire (confidentiality + informationStatus) —
        # mêmes valeurs que l'exemple officiel Datex II v2.3 (Accident with roadblocked.xml)
        header = ET.SubElement(situation, tag("headerInformation"))
        ET.SubElement(header, tag("confidentiality")).text = "noRestriction"
        ET.SubElement(header, tag("informationStatus")).text = "real"

        record = ET.SubElement(situation, tag("situationRecord"), {"id": f"SR-{props['id']}", "version": "1"})
        record.set(f"{{{XSI_NS}}}type", "EnvironmentalObstruction")

        # Ordre des éléments imposé par le schéma (xs:sequence) :
        # creationTime, versionTime, probabilityOfOccurrence, severity, validity,
        # generalPublicComment, groupOfLocations, puis les champs propres à
        # EnvironmentalObstruction (environmentalObstructionType) en dernier.
        ET.SubElement(record, tag("situationRecordCreationTime")).text = now_iso
        ET.SubElement(record, tag("situationRecordVersionTime")).text = now_iso
        ET.SubElement(record, tag("probabilityOfOccurrence")).text = "certain"
        ET.SubElement(record, tag("severity")).text = "medium"

        validity = ET.SubElement(record, tag("validity"))
        ET.SubElement(validity, tag("validityStatus")).text = "active" if props.get("statut_actif") else "suspended"
        time_spec = ET.SubElement(validity, tag("validityTimeSpecification"))
        # overallStartTime est obligatoire dans le schéma : repli sur date_saisie puis
        # sur l'heure de génération de l'export si aucune date exploitable (ex: CD35,
        # dont l'API ne fournit aucune date - cf. mémoire roadmap_objectif_md phase E).
        start_iso = (
            _fr_date_to_iso(props.get("date_debut"))
            or _fr_date_to_iso(props.get("date_saisie"))
            or now_iso
        )
        ET.SubElement(time_spec, tag("overallStartTime")).text = start_iso
        end_iso = _fr_date_to_iso(props.get("date_fin"))
        if end_iso:
            ET.SubElement(time_spec, tag("overallEndTime")).text = end_iso

        # route + commentaire regroupés ici : roadNumber n'existe pas dans le schéma
        # à ce niveau (référencement linéaire complet hors périmètre, cf. plan) —
        # le nom de route est donc intégré au texte plutôt que perdu.
        comment_text = " - ".join(t for t in (props.get("route"), props.get("commentaire")) if t)
        if comment_text:
            comment = ET.SubElement(record, tag("generalPublicComment"))
            comment_elem = ET.SubElement(comment, tag("comment"))
            values = ET.SubElement(comment_elem, tag("values"))
            value = ET.SubElement(values, tag("value"))
            value.set("lang", "fr")
            value.text = comment_text

        # PointByCoordinates n'hérite pas de GroupOfLocations dans le schéma : c'est un
        # type composant, à imbriquer dans Point (qui lui hérite bien de GroupOfLocations
        # via NetworkLocation -> Location). xsi:type doit donc porter sur "Point".
        group = ET.SubElement(record, tag("groupOfLocations"))
        group.set(f"{{{XSI_NS}}}type", "Point")
        point_by_coords = ET.SubElement(group, tag("pointByCoordinates"))
        point_coords = ET.SubElement(point_by_coords, tag("pointCoordinates"))
        ET.SubElement(point_coords, tag("latitude")).text = str(point[1])
        ET.SubElement(point_coords, tag("longitude")).text = str(point[0])

        ET.SubElement(record, tag("environmentalObstructionType")).text = "flooding"

    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    tree.write(output_path, encoding="utf-8", xml_declaration=True)
    print(f"Fichier {output_path} créé : {len(features)} signalement(s) exporté(s)")
    return len(features)


# ================================================================================
# CROISEMENT COMMUNAL (codes INSEE, via géocodage inverse IGN Géoplateforme)
# ================================================================================
#
# Même service que la recherche d'adresse déjà utilisée dans config.xml
# (data.geopf.fr/geocodage). Pas de référentiel BDTopo embarqué ni de shapely :
# un simple géocodage inverse par point, cohérent avec ce qui est déjà en
# confiance dans le projet.

REVERSE_GEOCODE_URL = "https://data.geopf.fr/geocodage/reverse"


def _reverse_geocode_commune(lon, lat):
    """Code INSEE de la commune au point donné, ou None si échec/absence.

    L'index "address" ne renvoie rien en zone sans adresse à proximité (routes
    rurales, forêts...) — confirmé en test réel (Brasparts, 29 : 0 résultat en
    address, résultat correct en parcel). Repli sur l'index "parcel" (parcelle
    cadastrale), qui couvre tout le territoire puisque chaque terrain en a une.
    "address" reste prioritaire quand il répond, pour rester cohérent avec le
    format le plus simple (citycode direct plutôt que department+municipality).

    Renvoie (code_insee, nom_commune) : les deux index renvoient déjà un champ
    "city" (vérifié sur les deux), donc le nom ne coûte pas de requête
    supplémentaire - utile pour labelliser les communes secondaires d'un
    tronçon (cf aggregate_by_commune() dans stats_prefecture.py), que la
    source elle-même ne nomme jamais puisqu'elle ne déclare qu'un seul champ
    commune (texte libre, généralement celui du point de départ)."""
    try:
        resp = requests.get(
            REVERSE_GEOCODE_URL,
            params={"lon": lon, "lat": lat, "index": "address", "limit": 1},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=HTTP_TIMEOUT,
        )
        if resp.ok:
            features = resp.json().get("features") or []
            if features:
                props = features[0].get("properties", {})
                citycode = props.get("citycode")
                if citycode:
                    return citycode, props.get("city")

        resp = requests.get(
            REVERSE_GEOCODE_URL,
            params={"lon": lon, "lat": lat, "index": "parcel", "limit": 1},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=HTTP_TIMEOUT,
        )
        if not resp.ok:
            return None, None
        features = resp.json().get("features") or []
        if not features:
            return None, None
        props = features[0].get("properties", {})
        dept, municipality = props.get("departmentcode"), props.get("municipalitycode")
        if not dept or not municipality:
            return None, None
        return f"{dept}{municipality}", props.get("city")
    except requests.RequestException:
        return None, None


def _sample_points_for_commune_lookup(geometry):
    """Points à interroger pour déterminer les communes traversées.
    Pour une ligne, début/milieu/fin du tracé — même simplification que
    _representative_point() pour l'export Datex II."""
    geom_type = geometry.get("type")
    coords = geometry.get("coordinates")
    if geom_type == "Point" and coords:
        return [coords]
    if geom_type == "LineString" and coords:
        return [coords[0], coords[len(coords) // 2], coords[-1]]
    if geom_type == "MultiLineString" and coords and coords[0]:
        line = coords[0]
        return [line[0], line[len(line) // 2], line[-1]]
    return []


def enrich_with_communes(features):
    """Ajoute properties.codes_insee et properties.noms_communes (listes
    parallèles, même ordre/longueur) à chaque feature. noms_communes vient de
    notre propre géocodage inverse, pas de la source - c'est ce qui permet de
    labelliser une commune secondaire d'un tronçon que la source ne nomme
    jamais.

    properties.commune (texte libre déclaré par la source, une seule valeur)
    n'est réécrit que si le tracé touche RÉELLEMENT plusieurs communes
    distinctes (cas des lignes uniquement - un point n'a qu'un seul point
    échantillonné donc jamais plus d'un nom) : il devient alors la
    concaténation de toutes les communes traversées ("Mordelles / Bréal-sous-
    Montfort"), affichée telle quelle par le popup mviewer. Sinon inchangé,
    pour ne pas perdre une précision que la source aurait donnée (lieu-dit,
    ancien nom de commune...)."""
    print("\nCroisement communal (codes INSEE)...")

    def lookup(feature):
        codes, noms = [], []
        for lon, lat in _sample_points_for_commune_lookup(feature["geometry"]):
            code, nom = _reverse_geocode_commune(lon, lat)
            if code and code not in codes:
                codes.append(code)
                noms.append(nom or "")
        return codes, noms

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(lookup, features))

    for feature, (codes, noms) in zip(features, results):
        feature["properties"]["codes_insee"] = codes
        feature["properties"]["noms_communes"] = noms
        distinct_noms = list(dict.fromkeys(n for n in noms if n))
        if len(distinct_noms) > 1:
            feature["properties"]["commune"] = " / ".join(distinct_noms)

    total_found = sum(1 for codes, _ in results if codes)
    print(f"   {total_found}/{len(features)} signalement(s) rattachés à une commune")


# ================================================================================
# EXPORT CSV (grand public, toutes causes, sans géométrie)
# ================================================================================

CSV_FIELDS = [
    "id", "id_source", "source", "route", "commune", "codes_insee", "cause", "statut",
    "type_coupure", "sens_circulation", "commentaire", "date_debut", "date_fin",
    "date_saisie", "gestionnaire",
]


def build_csv_export(features, output_path="signalements_inondation.csv"):
    # utf-8-sig (BOM) plutôt que utf-8 : sans le BOM, Excel — l'outil le plus
    # probable pour ouvrir un CSV grand public — ne détecte pas l'UTF-8 et
    # affiche les accents cassés. Le BOM est invisible/sans effet ailleurs.
    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for feature in features:
            row = dict(feature["properties"])
            row["codes_insee"] = ";".join(row.get("codes_insee") or [])
            writer.writerow(row)
    print(f"Fichier {output_path} créé : {len(features)} signalement(s) exporté(s)")


# ================================================================================
# FUSION PRINCIPALE
# ================================================================================


def merge_sources():
    try:
        print()

        def _submit_or_skip(executor, name, fetch_fn):
            if name in DISABLED_SOURCES:
                print(f"   {name}: désactivée via DISABLED_SOURCES, source non interrogée")
                return None
            return executor.submit(monitor_fetch, name, fetch_fn)

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            futures = {
                "grist_35": _submit_or_skip(executor, "grist_35", fetch_grist_data),
                "cd44": _submit_or_skip(executor, "cd44", fetch_cd44_data),
                "rennes_metropole": _submit_or_skip(executor, "rennes_metropole", fetch_rennes_metro_data),
                "cd35_inondations": _submit_or_skip(executor, "cd35_inondations", fetch_cd35_inondations_data),
                "cd35_lineaire": _submit_or_skip(executor, "cd35_lineaire", fetch_cd35_lineaire_data),
                "cd56": _submit_or_skip(executor, "cd56", fetch_cd56_data),
            }
            results = {name: (f.result() if f is not None else None) for name, f in futures.items()}

        if "diro" in DISABLED_SOURCES:
            print("   diro: désactivée via DISABLED_SOURCES, source non interrogée")
            diro_features = []
        else:
            diro_features = monitor_fetch("diro", fetch_diro_data) or []

        grist_records = results["grist_35"] or []
        cd44_records = results["cd44"] or []
        cd35_features = results["cd35_inondations"] or []
        cd35_lineaire_features = results["cd35_lineaire"] or []
        cd56_features = results["cd56"] or []

        rennes_result = results["rennes_metropole"]
        if isinstance(rennes_result, dict):
            rennes_features = rennes_result.get("features") or []
            needs_conversion = rennes_result.get("needsConversion", False)
        else:
            rennes_features = []
            needs_conversion = False

        total_brut = (
            len(grist_records) + len(cd44_records) + len(rennes_features)
            + len(cd35_features) + len(cd35_lineaire_features) + len(cd56_features) + len(diro_features)
        )
        print(f"\nTotal brut récupéré: {total_brut} records\n")

        features = []
        stats = {
            "grist_recupere": len(grist_records), "grist_garde": 0,
            "cd44_recupere": len(cd44_records), "cd44_garde": 0,
            "rennes_recupere": len(rennes_features), "rennes_garde": 0,
            "cd35_recupere": len(cd35_features), "cd35_garde": 0,
            "cd35_lineaire_recupere": len(cd35_lineaire_features), "cd35_lineaire_garde": 0,
            "cd56_recupere": len(cd56_features), "cd56_garde": 0,
            "diro_recupere": len(diro_features), "diro_garde": 0,
            "resolus_filtres": 0,
        }

        def _process(raw_items, convert_fn, key, label):
            for item in raw_items:
                feature = convert_fn(item)
                if feature is None:
                    continue
                result = should_keep_feature(feature)
                if result.keep:
                    features.append(feature)
                    stats[f"{key}_garde"] += 1
                elif result.filtered_resolved:
                    stats["resolus_filtres"] += 1
            print(f"   {label}: {stats[f'{key}_recupere']} récupérés → {stats[f'{key}_garde']} gardés")

        _process(grist_records, grist_to_feature, "grist", "Grist 35")
        _process(cd44_records, cd44_to_feature, "cd44", "CD44")
        _process(rennes_features, lambda f: rennes_metro_to_feature(f, needs_conversion), "rennes", "Rennes Métropole")
        _process(cd35_features, cd35_inondations_to_feature, "cd35", "CD35")
        _process(cd35_lineaire_features, cd35_lineaire_to_feature, "cd35_lineaire", "CD35 Linéaire")
        _process(cd56_features, cd56_to_feature, "cd56", "CD56")
        _process(diro_features, diro_to_feature, "diro", "DIRO")

        total_garde = sum(stats[f"{k}_garde"] for k in ("grist", "cd44", "rennes", "cd35", "cd35_lineaire", "cd56", "diro"))
        total_filtre = total_brut - total_garde

        enrich_with_communes(features)

        print("\nArchivage annuel...")
        for feature in features:
            add_or_update_in_archive(feature)
        detect_deleted_signalements(features)
        print("Archivage terminé\n")

        print("\nRésumé:")
        print(f"   Total récupéré: {total_brut}")
        print(f"   Total gardé: {total_garde}")
        print(f"   Total filtré: {total_filtre}")
        print(f"   → dont résolus > 3 jours: {stats['resolus_filtres']}\n")

        geojson = {
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "generated": _iso_now(),
                "source": "Fusion Grist 35 + CD44 + Rennes Métropole + CD35 Inondations + CD35 Inondations Linéaire + CD56 + DIRO",
                "total_count": len(features),
                "sources": {
                    "grist_35": len(grist_records),
                    "cd44": len(cd44_records),
                    "rennes_metropole": len(rennes_features),
                    "cd35_inondations": len(cd35_features),
                    "cd35_lineaire": len(cd35_lineaire_features),
                    "cd56": len(cd56_features),
                    "diro": len(diro_features),
                },
            },
        }
        with open("signalements.geojson", "w", encoding="utf-8") as f:
            json.dump(geojson, f, indent=2, ensure_ascii=False)
        print("Fichier signalements.geojson créé")

        build_datex2_export(features)
        build_csv_export(features)

        date_time_fr = get_datetime_fr()

        administrations = {}
        for feature in features:
            admin = feature["properties"].get("administration") or "Non spécifié"
            administrations[admin] = administrations.get(admin, 0) + 1

        par_source = {
            "saisie_grist": sum(1 for f in features if f["properties"]["source"] == "Saisie Grist"),
            "cd44": sum(1 for f in features if f["properties"]["source"] == "CD44"),
            "rennes_metropole": sum(1 for f in features if f["properties"]["source"] == "Rennes Métropole"),
            "cd35_inondations": sum(1 for f in features if f["properties"]["source"] == "CD35 Inondations"),
            "cd35_lineaire": sum(1 for f in features if f["properties"]["source"] == "CD35 Inondations Linéaire"),
            "cd56": sum(1 for f in features if f["properties"]["source"] == "CD56"),
            "diro": sum(1 for f in features if f["properties"]["source"] == "DIRO"),
        }

        summary = {"total": 7, "ok": 0, "empty": 0, "error": 0}
        for status in flux_monitor.values():
            if status:
                if status["status"] == "OK":
                    summary["ok"] += 1
                elif status["status"] == "EMPTY":
                    summary["empty"] += 1
                elif status["status"] == "ERROR":
                    summary["error"] += 1

        global_status = "OK"
        if summary["error"] > 0:
            global_status = "CRITICAL"
        elif summary["empty"] > 0:
            global_status = "DEGRADED"

        metadata = {
            "lastUpdate": date_time_fr["iso"],
            "lastUpdateFR": date_time_fr["local"],
            "timezone": date_time_fr["timezone"],
            "nextUpdateIn": "30 minutes",
            "totalRecus": total_brut,
            "totalInclus": total_garde,
            "totalFiltres": total_filtre,
            "resolus_filtres_3jours": stats["resolus_filtres"],
            "sources_recues": {
                "grist_35": len(grist_records),
                "cd44": len(cd44_records),
                "rennes_metropole": len(rennes_features),
                "cd35_inondations": len(cd35_features),
                "cd35_lineaire": len(cd35_lineaire_features),
                "cd56": len(cd56_features),
                "diro": len(diro_features),
            },
            "sources_incluses": par_source,
            "geometries": {
                "points": sum(1 for f in features if f["geometry"]["type"] == "Point"),
                "lignes": sum(1 for f in features if f["geometry"]["type"] == "LineString"),
                "multilignes": sum(1 for f in features if f["geometry"]["type"] == "MultiLineString"),
                "polygones": sum(1 for f in features if f["geometry"]["type"] == "Polygon"),
            },
            "administrations": administrations,
            "archives": {
                "enabled": True,
                "location": "archives/",
                "description": "Historique annuel permanent (toutes sources)",
                "note": "Les signalements sont archivés par année (date_debut) et suivis pour détecter les suppressions",
            },
            "flux_monitoring": {
                "globalStatus": global_status,
                "lastCheck": date_time_fr["local"],
                "lastCheckISO": date_time_fr["iso"],
                "summary": summary,
                "sources": flux_monitor,
            },
        }

        with open("metadata.json", "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        print("Métadonnées créées (avec monitoring des flux intégré)")

        print("\nMonitoring des flux:")
        print(f"   Statut global: {global_status}")
        print(f"   OK: {summary['ok']}")
        print(f"   VIDE: {summary['empty']}")
        print(f"   ERREUR: {summary['error']}")

        print("\nStatistiques finales:")
        print(f"   - Heure mise à jour: {date_time_fr['local']}")
        print(f"   - Total reçu: {total_brut}")
        print(f"   - Total inclus: {total_garde}")
        print(f"   - Total filtré: {total_filtre}")
        print(f"   - Grist 35: {len(grist_records)}")
        print(f"   - CD44: {len(cd44_records)}")
        print(f"   - Rennes Métropole: {len(rennes_features)}")
        print(f"   - CD35 Inondations: {len(cd35_features)}")
        print(f"   - CD35 Inondations Linéaire: {len(cd35_lineaire_features)}")
        print(f"   - CD56: {len(cd56_features)}")
        print(f"   - DIRO: {len(diro_features)}")
        print(f"   - Points: {metadata['geometries']['points']}")
        print(f"   - LineStrings: {metadata['geometries']['lignes']}")
        print(f"   - Polygons: {metadata['geometries']['polygones']}")
        print("\nPar administration:")
        for admin, count in administrations.items():
            print(f"   - {admin}: {count}")

        print("\nScript terminé avec succès\n")

    except Exception as exc:
        print(f"Erreur fusion: {exc}")
        raise SystemExit(1)


if __name__ == "__main__":
    print("Démarrage de la fusion des 7 sources...\n")
    print("   1. Grist 35 (signalements manuels)")
    print("   2. CD44 (API REST)")
    print("   3. Rennes Métropole (WFS routes coupées)")
    print("   4. CD35 Inondations (API OGC)")
    print("   5. CD35 Inondations Linéaire (API OGC)")
    print("   6. CD56 (API OGC)")
    print("   7. DIRO - DIR Ouest (DATEX II)\n")
    merge_sources()
