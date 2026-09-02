#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Export des stations hydrométriques Hub'Eau, positions + dernière hauteur
d'eau connue (chantier §10 - croisement historique avec les coupures).

Interroge l'API Hub'Eau Hydrométrie (hubeau.eaufrance.fr, gratuite, sans
clé) pour les stations en service des départements 35/44/56 (même périmètre
que le reste du projet), enrichit chaque station avec son dernier relevé de
hauteur d'eau, et écrit deux choses :
  - un instantané du jour (stations_hydro.geojson, écrasé à chaque run,
    pour l'exploration QGIS - comportement historique inchangé) ;
  - une accumulation annuelle additive (hauteurs_eau_<année>.geojson,
    jamais écrasée, une ligne par relevé) qui alimente le futur croisement
    coupures/hauteurs d'eau (hors périmètre de ce script).

Déclenchement : le script ne s'exécute que si vigilance Vigicrues
(vigilance.json, déjà publié par fetch_vigicrues.py) au moins jaune OU au
moins une coupure de route active existe (signalements.geojson, déjà
publié par merge_signalements.py) - une crue locale peut couper une route
sans faire monter le niveau de vigilance des 7 tronçons suivis. Prévu pour
un schedule 2x/jour, 10h et 16h (JOB_NAME=stations-hydro, cf
.gitlab-ci.yml).

Chaque station est aussi taguée avec vigilance_niveau (vert/jaune/orange/
rouge, ou None si hors du réseau de vigilance crues Vigicrues), via
STATION_TRONCON - une correspondance station Hub'Eau -> tronçon Vigicrues,
figée en dur (cf commentaire de STATION_TRONCON). Mis en place pour
l'avenir uniquement : les relevés déjà accumulés dans
hauteurs_eau_<année>.geojson avant ce changement n'ont pas ce champ, et ne
sont jamais retouchés rétroactivement.
"""

import json
import os
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests

# ================================================================================
# CONFIGURATION
# ================================================================================

HUBEAU_STATIONS_URL = "https://hubeau.eaufrance.fr/api/v2/hydrometrie/referentiel/stations"
HUBEAU_OBSERVATIONS_URL = "https://hubeau.eaufrance.fr/api/v2/hydrometrie/observations_tr"
DEPARTEMENTS = ["35", "44", "56"]

# Mêmes fichiers que fetch_vigicrues.py / merge_signalements.py, déjà
# publiés - on les relit pour ne lancer ce relevé que si l'un des deux
# déclencheurs le justifie (vigilance jaune+, ou une coupure de route
# active même sans vigilance). Les deux URLs pointent vers data/github/ (pas
# data/gitlab/, ni le chemin partagé pour vigilance.json) : ce dépôt doit
# toujours lire ce que SES PROPRES jobs viennent de publier, jamais la
# sortie GitLab - qui ne serait plus rafraîchie si gitlab-forge tombe, et
# que ce pipeline ne doit jamais écraser non plus (cf fetch_vigicrues.py).
VIGILANCE_URL = "https://geobretagne.fr/apps/routes_coupees_inondation/data/github/vigilance.json"
SIGNALEMENTS_URL = "https://geobretagne.fr/apps/routes_coupees_inondation/data/github/signalements.geojson"
NIVEAUX = {"vert": 1, "jaune": 2, "orange": 3, "rouge": 4}
SEUIL_NIVEAU = NIVEAUX["jaune"]

OUTPUT_FILE = "stations_hydro.geojson"
ACCUMULATION_TEMPLATE = "hauteurs_eau_{year}.geojson"
HTTP_TIMEOUT = 30


# ================================================================================
# DÉCLENCHEMENT (vigilance jaune+ OU coupure de route active)
# ================================================================================


def fetch_vigilance_data():
    """Contenu de vigilance.json (déjà publié par fetch_vigicrues.py), ou None
    en cas d'erreur. Lu une seule fois par run et partagé entre
    vigilance_suffisante() (déclenchement) et tag_vigilance() (couleur par
    station) plutôt que refait deux fois."""
    try:
        response = requests.get(VIGILANCE_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        print(f"Erreur lecture vigilance.json ({exc})")
        return None


def vigilance_suffisante(vigilance_data):
    """True si le niveau de vigilance max actuel est jaune ou plus. En cas
    d'erreur de lecture (vigilance_data=None), on part du principe qu'il vaut
    mieux exporter que rater un vrai événement (échoue "ouvert", pas
    "fermé")."""
    if vigilance_data is None:
        print("vigilance.json indisponible - export lancé par précaution")
        return True
    niveau_max_code = vigilance_data.get("niveau_max_code")
    print(f"Vigilance actuelle : {vigilance_data.get('niveau_max')} (code {niveau_max_code})")
    return niveau_max_code is not None and niveau_max_code >= SEUIL_NIVEAU


def coupure_active_existe():
    """True si au moins un signalement actif figure dans signalements.geojson
    déjà publié (une crue locale peut couper une route sans faire monter la
    vigilance des 7 tronçons Vigicrues suivis). Même philosophie fail-open
    que vigilance_suffisante() : une erreur de lecture ici ne doit pas
    empêcher un relevé si l'autre déclencheur, lui, est correctement
    détecté - et en cas de double échec, mieux vaut un relevé de trop
    qu'un relevé manqué un jour d'inondation."""
    try:
        response = requests.get(SIGNALEMENTS_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        features = response.json().get("features") or []
        actifs = sum(1 for f in features if f.get("properties", {}).get("statut_actif") is True)
        print(f"Signalements actifs actuels : {actifs}")
        return actifs > 0
    except Exception as exc:
        print(f"Erreur lecture signalements.geojson ({exc}) - relevé lancé par précaution")
        return True


def determine_declencheurs(vigilance_data):
    """Liste des raisons qui justifient le relevé de ce run (une ou deux
    valeurs). Vide seulement si ni la vigilance ni une coupure active ne
    justifient de tourner aujourd'hui."""
    declencheurs = []
    if vigilance_suffisante(vigilance_data):
        declencheurs.append("vigilance_jaune")
    if coupure_active_existe():
        declencheurs.append("coupure_active")
    return declencheurs


# ================================================================================
# VIGILANCE PAR STATION (mise en place pour l'avenir uniquement)
# ================================================================================
#
# Objectif.md ne demande la vigilance que par tronçon (7 tronçons du bassin
# Vilaine, cf fetch_vigicrues.py) - Hub'Eau ne fournit aucun lien station <->
# tronçon (vérifié sur un vrai enregistrement de station, aucun champ de ce
# type). La correspondance existe côté Vigicrues lui-même : son API
# officielle (non documentée publiquement, cf. www.vigicrues.gouv.fr/services/v1.1)
# expose pour chaque tronçon la liste de ses stations, sous
# TronEntVigiCru.json?CdEntVigiCru=<code_troncon>&TypEntVigiCru=8 ->
# aNMoinsUn[].CdEntVigiCruInferieur - vérifié en confrontant plusieurs codes
# renvoyés (ex. J701061001) au référentiel Hub'Eau local
# (scripts/stations_hydro.geojson) : c'est exactement le même code_station
# des deux côtés.
#
# Figée en dur ici (52 stations sur les 113 en service 35/44/56, le reste
# étant sur des rivières hors du réseau de vigilance crues Vigicrues de ce
# bassin - la Chère, l'Erdre, l'Aff, la Loire...) plutôt qu'interrogée à
# chaque run : cette association ne change quasiment jamais côté Vigicrues,
# et l'API utilisée pour la construire est capricieuse (une combinaison de
# paramètres invalide renvoie juste "Problème d'exécution du process", sans
# détail) - pas une dépendance à ajouter à un job qui tourne 2x/jour. À
# reconstruire à la main (mêmes appels) si Vigicrues redécoupe un jour ses
# tronçons.
STATION_TRONCON = {
    # Vilaine amont (BT9)
    "J701064001": "Vilaine amont", "J702401001": "Vilaine amont", "J701061001": "Vilaine amont",
    "J700061001": "Vilaine amont", "J705302001": "Vilaine amont", "J709063002": "Vilaine amont",
    "J704301001": "Vilaine amont", "J708311001": "Vilaine amont", "J702403001": "Vilaine amont",
    "J706062001": "Vilaine amont", "J702402001": "Vilaine amont",
    # Vilaine médiane (BT8)
    "J750061001": "Vilaine médiane", "J763301001": "Vilaine médiane", "J721401001": "Vilaine médiane",
    "J770061002": "Vilaine médiane", "J770061001": "Vilaine médiane", "J751301001": "Vilaine médiane",
    # Vilaine aval (BT6)
    "J900001001": "Vilaine aval", "J783301030": "Vilaine aval", "J795301010": "Vilaine aval",
    "J798061001": "Vilaine aval", "J900061001": "Vilaine aval", "J782401001": "Vilaine aval",
    "J797301001": "Vilaine aval",
    # Ille (BT11)
    "J711401001": "Ille", "J712301001": "Ille", "J710301001": "Ille", "J712001001": "Ille",
    # Meu (BT10)
    "J739301001": "Meu", "J734401001": "Meu", "J735301001": "Meu", "J731301001": "Meu",
    "J736422001": "Meu",
    # Seiche (BT12)
    "J747000101": "Seiche", "J748301002": "Seiche", "J744301001": "Seiche",
    # Oust (BT7)
    "J800231002": "Oust", "J820234001": "Oust", "J800232001": "Oust", "J820231002": "Oust",
    "J850231002": "Oust", "J840231001": "Oust", "J863241001": "Oust", "J850231003": "Oust",
    "J802231003": "Oust", "J813301001": "Oust", "J836311001": "Oust", "J843301002": "Oust",
    "J844301001": "Oust", "J881301001": "Oust", "J860241001": "Oust", "J833301002": "Oust",
}


def tag_vigilance(features, vigilance_data):
    """Ajoute vigilance_niveau (vert/jaune/orange/rouge, ou None) à chaque
    station selon le niveau ACTUEL de son tronçon - jamais recalculé
    rétroactivement pour les relevés déjà accumulés dans
    hauteurs_eau_<année>.geojson, seulement pour ceux écrits à partir de
    maintenant (les relevés déjà écrits n'ont pas ce champ, l'affichage doit
    les traiter comme "vigilance inconnue", pas comme "vert")."""
    troncons = (vigilance_data or {}).get("troncons") or {}
    tagged = 0
    for feature in features:
        code_station = feature["properties"].get("code_station")
        troncon = STATION_TRONCON.get(code_station)
        info = troncons.get(troncon) if troncon else None
        niveau = info.get("niveau") if info else None
        feature["properties"]["vigilance_niveau"] = niveau
        if niveau:
            tagged += 1
    print(f"Vigilance par station : {tagged}/{len(features)} station(s) rattachée(s) à un tronçon suivi")


# ================================================================================
# RÉCUPÉRATION
# ================================================================================


def fetch_stations_departement(code):
    """Stations en service d'un département, au format GeoJSON natif Hub'Eau
    (un seul appel suffit : size=1000 couvre largement les ~30-50 stations
    actives par département constatées sur 35/44/56)."""
    params = {
        "code_departement": code,
        "en_service": "true",
        "format": "geojson",
        "size": 1000,
    }
    response = requests.get(HUBEAU_STATIONS_URL, params=params, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    return response.json().get("features") or []


def collect_stations():
    """Interroge chaque département et retourne les features fusionnées.
    Un département en échec n'empêche pas les autres (même tolérance aux
    pannes que collect_niveaux() dans fetch_vigicrues.py)."""
    features = []
    erreurs = {}

    for code in DEPARTEMENTS:
        try:
            dep_features = fetch_stations_departement(code)
            features.extend(dep_features)
            print(f"[{code}] {len(dep_features)} station(s) active(s)")
        except Exception as exc:
            erreurs[code] = str(exc)
            print(f"[{code}] erreur - {exc}")

    if not features:
        raise RuntimeError("Aucune station n'a pu être récupérée")

    return features, erreurs


# ================================================================================
# HAUTEUR D'EAU (dernier relevé par station)
# ================================================================================


HAUTEUR_TENTATIVES = 3
HAUTEUR_PAUSE_SECONDES = 2


def fetch_latest_hauteur(code_station):
    """Dernier relevé de hauteur d'eau (mm) d'une station, ou (None, None) si
    aucune donnée H récente (ex: station à mesure manuelle, sans
    télétransmission - ce cas répond immédiatement, pas de nouvelle
    tentative nécessaire). Les résultats de observations_tr sont triés du
    plus récent au plus ancien, donc size=1 suffit à obtenir la dernière
    valeur.

    Sur 113 stations interrogées à la suite, Hub'Eau met occasionnellement
    plus de HTTP_TIMEOUT à répondre à un appel isolé sans que ce soit une
    vraie panne (constaté : la donnée était bien disponible en retestant
    juste après) - on retente donc quelques fois avant d'abandonner, plutôt
    que de perdre définitivement une station à cause d'un aléa réseau."""
    params = {"code_entite": code_station, "grandeur_hydro": "H", "size": 1}

    for tentative in range(1, HAUTEUR_TENTATIVES + 1):
        try:
            response = requests.get(HUBEAU_OBSERVATIONS_URL, params=params, timeout=HTTP_TIMEOUT)
            response.raise_for_status()
            data = response.json().get("data") or []
            if not data:
                return None, None
            return data[0]["resultat_obs"], data[0]["date_obs"]
        except requests.RequestException:
            if tentative == HAUTEUR_TENTATIVES:
                raise
            time.sleep(HAUTEUR_PAUSE_SECONDES)


def enrich_with_hauteur(features):
    """Ajoute hauteur_eau_mm/date_mesure_hauteur à chaque station. Une
    station en erreur ou sans donnée H récente garde ces champs à null,
    sans interrompre les autres (un appel HTTP par station)."""
    sans_hauteur = 0

    for feature in features:
        code_station = feature["properties"].get("code_station")
        try:
            hauteur_mm, date_mesure = fetch_latest_hauteur(code_station)
        except Exception as exc:
            print(f"[{code_station}] erreur hauteur - {exc}")
            hauteur_mm, date_mesure = None, None

        feature["properties"]["hauteur_eau_mm"] = hauteur_mm
        feature["properties"]["date_mesure_hauteur"] = date_mesure
        if hauteur_mm is None:
            sans_hauteur += 1

    print(f"Hauteur d'eau récupérée pour {len(features) - sans_hauteur}/{len(features)} station(s)")
    if sans_hauteur:
        print(f"   {sans_hauteur} station(s) sans donnée H récente (débit uniquement, ou en panne)")


# ================================================================================
# EXPORT
# ================================================================================

# Le référentiel Hub'Eau renvoie une trentaine de champs par station, en
# grande partie des codes techniques SANDRE peu lisibles - on ne garde que
# l'essentiel pour l'exploration (QGIS).
CHAMPS_A_GARDER = [
    "code_station",
    "libelle_station",
    "libelle_commune",
    "code_commune_station",
    "libelle_cours_eau",
    "en_service",
    "hauteur_eau_mm",
    "date_mesure_hauteur",
    "vigilance_niveau",
]


def simplify_properties(features):
    for feature in features:
        props = feature["properties"]
        feature["properties"] = {champ: props.get(champ) for champ in CHAMPS_A_GARDER}


def _iso_now():
    """Équivalent de Date.toISOString() en JS : UTC, millisecondes, suffixe Z
    (même helper que merge_signalements.py)."""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def write_snapshot(features):
    """Instantané du jour, écrasé à chaque run - comportement historique
    inchangé, utile pour l'exploration QGIS."""
    geojson = {"type": "FeatureCollection", "features": features}
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False)
    print(f"\nFichier {OUTPUT_FILE} créé : {len(features)} station(s)")


# ================================================================================
# ACCUMULATION ANNUELLE (hauteurs d'eau, jamais écrasée)
# ================================================================================


def load_accumulation(year):
    """Télécharge/lit l'accumulation de l'année en cours si elle existe déjà
    (le fichier est re-téléchargé depuis le WebDAV avant le run par
    .gitlab-ci.yml, comme les archives de signalements) - sinon repart d'une
    collection vide plutôt que d'échouer."""
    path = ACCUMULATION_TEMPLATE.format(year=year)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as exc:
            print(f"Erreur lecture {path}, création nouvelle: {exc}")
    return {
        "type": "FeatureCollection",
        "features": [],
        "metadata": {"year": year, "created": _iso_now(), "last_update": _iso_now()},
    }


def save_accumulation(year, geojson):
    path = ACCUMULATION_TEMPLATE.format(year=year)
    geojson["metadata"]["last_update"] = _iso_now()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False)


def append_releves(features, declencheurs):
    """Ajoute au fichier annuel un relevé par station ayant une hauteur
    connue, sans jamais écraser les relevés précédents. Pas de
    déduplication : chaque run ajoute ses lignes même si la valeur Hub'Eau
    est identique au run précédent (le capteur n'a pas forcément été
    remis à jour entre 10h et 16h) - décision explicite de l'utilisatrice,
    pour garder la trace que le pipeline a bien vérifié à chaque heure."""
    year = datetime.now(ZoneInfo("Europe/Paris")).year
    accumulation = load_accumulation(year)

    ajoutes = 0
    for feature in features:
        props = feature["properties"]
        if props.get("hauteur_eau_mm") is None:
            continue
        accumulation["features"].append({
            "type": "Feature",
            "geometry": feature["geometry"],
            "properties": {**props, "declencheurs": declencheurs, "date_releve_pipeline": _iso_now()},
        })
        ajoutes += 1

    save_accumulation(year, accumulation)
    print(f"{ajoutes} relevé(s) ajouté(s) à {ACCUMULATION_TEMPLATE.format(year=year)} "
          f"({len(accumulation['features'])} au total)")


# ================================================================================
# POINT D'ENTRÉE
# ================================================================================


def main():
    vigilance_data = fetch_vigilance_data()
    declencheurs = determine_declencheurs(vigilance_data)
    if not declencheurs:
        print("Ni vigilance jaune+, ni coupure active - rien à exporter aujourd'hui.")
        return
    print(f"Déclenchement : {', '.join(declencheurs)}")

    print("\nRécupération des stations hydrométriques Hub'Eau (35/44/56)...\n")
    features, erreurs = collect_stations()

    print("\nRécupération des hauteurs d'eau...")
    enrich_with_hauteur(features)
    tag_vigilance(features, vigilance_data)
    simplify_properties(features)

    write_snapshot(features)
    append_releves(features, declencheurs)

    if erreurs:
        print(f"\nDépartement(s) en erreur : {erreurs}")


if __name__ == "__main__":
    main()
