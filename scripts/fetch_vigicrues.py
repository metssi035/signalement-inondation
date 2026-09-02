#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de récupération du niveau de vigilance crues Vigicrues.

Interroge les flux RSS Vigicrues des tronçons du bassin de la Vilaine
(territoire "Vilaine-Côtiers Bretons"), et calcule le niveau de vigilance
maximal parmi eux. Écrit un fichier JSON exploitable par le visualiseur
pour afficher dynamiquement le niveau de vigilance dans son sous-titre.
"""

import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import requests

# ============================================================================
# CONFIGURATION
# ============================================================================

VIGICRUES_RSS_URL = "https://www.vigicrues.gouv.fr/territoire/rss"

# Tronçons du bassin de la Vilaine (territoire Vigicrues "Vilaine-Côtiers Bretons")
TRONCONS = {
    "BT9": "Vilaine amont",
    "BT8": "Vilaine médiane",
    "BT6": "Vilaine aval",
    "BT11": "Ille",
    "BT10": "Meu",
    "BT12": "Seiche",
    "BT7": "Oust",
}

NIVEAUX = {"vert": 1, "jaune": 2, "orange": 3, "rouge": 4}

OUTPUT_FILE = "data/vigilance.json"
HTTP_TIMEOUT = 30


# ============================================================================
# RÉCUPÉRATION ET PARSING
# ============================================================================


def fetch_troncon_niveau(code):
    """Récupère le niveau de vigilance d'un tronçon via son flux RSS.

    Le flux contient un seul <item> dont le <title> a la forme
    "Nom du tronçon : couleur" (ex: "Vilaine amont : vert").
    """
    url = f"{VIGICRUES_RSS_URL}?CdEntVigiCru={code}"
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=HTTP_TIMEOUT)
    response.raise_for_status()

    root = ET.fromstring(response.content)
    title_elem = root.find(".//item/title")
    if title_elem is None or not title_elem.text:
        raise ValueError("Pas de <title> dans le flux RSS")

    _, _, niveau = title_elem.text.rpartition(":")
    niveau = niveau.strip().lower()
    if niveau not in NIVEAUX:
        raise ValueError(f"Niveau de vigilance inattendu : '{niveau}'")

    return niveau


def collect_niveaux():
    """Interroge tous les tronçons et retourne (détail, niveau max, erreurs)."""
    troncons_result = {}
    erreurs = {}

    for code, nom in TRONCONS.items():
        try:
            niveau = fetch_troncon_niveau(code)
            troncons_result[nom] = {"code": code, "niveau": niveau}
            print(f"[{nom}] ({code}) : {niveau}")
        except Exception as exc:
            erreurs[nom] = str(exc)
            print(f"[{nom}] ({code}) : erreur - {exc}")

    if not troncons_result:
        raise RuntimeError("Aucun tronçon n'a pu être récupéré")

    niveau_max = max(troncons_result.values(), key=lambda t: NIVEAUX[t["niveau"]])["niveau"]

    return troncons_result, niveau_max, erreurs


# ============================================================================
# EXPORT
# ============================================================================


def write_output(troncons_result, niveau_max, erreurs):
    now = datetime.now(timezone.utc)
    generated_iso = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"

    data = {
        "generated": generated_iso,
        "niveau_max": niveau_max,
        "niveau_max_code": NIVEAUX[niveau_max],
        "troncons": troncons_result,
    }
    if erreurs:
        data["erreurs"] = erreurs

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"\nFichier {OUTPUT_FILE} créé : niveau max = {niveau_max}")


# ============================================================================
# POINT D'ENTRÉE
# ============================================================================


def main():
    print("Récupération de la vigilance crues Vigicrues (bassin Vilaine)...\n")
    troncons_result, niveau_max, erreurs = collect_niveaux()
    write_output(troncons_result, niveau_max, erreurs)


if __name__ == "__main__":
    main()
