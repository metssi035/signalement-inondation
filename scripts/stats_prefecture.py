#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Statistiques de coupures de routes par commune, à destination de la
préfecture.

Indépendant de merge_signalements.py : relit signalements.geojson déjà
publié (donc pas besoin des données brutes ni des identifiants des flux
sources), agrège les coupures actives par commune via properties.codes_insee
et properties.commune (déjà calculés par merge_signalements.py), et écrit le
résultat dans un document Grist dédié (org "inforoute" sur
grist.dataregion.fr) :
  - table Situation_actuelle : vidée puis réécrite à chaque run, une ligne
    par commune touchée + une ligne "Total" ;
  - table Historique_signalements : un vrai historique par point/tronçon
    (une ligne = un signalement, identifié par source + id_source), pas par
    commune — une ligne apparaît quand un signalement devient actif, et se
    complète (date_reouverture) quand il n'est plus vu actif. L'état "encore
    ouvert ou déjà refermé" est déduit de Grist lui-même à chaque run (pas de
    fichier d'état local, qui ne survivrait pas à un conteneur CI éphémère).

Le périmètre géographique pris en compte est celui de masque.geojson (même
fichier et même règle géométrique que le visualiseur, cf isInPerimeter()
dans featurestyles-signalements.js) — un signalement dont le point
représentatif tombe hors de ce périmètre est ignoré.

Répartition par organisme pour "Saisie Grist" : ce flux mélange en réalité
plusieurs organismes distincts (aujourd'hui seule Redon Agglomération saisit
via Grist, mais properties.gestionnaire — un champ à choix défini par
l'utilisatrice dans le doc Grist source — est prévu pour en accueillir
d'autres). Plutôt qu'une seule colonne nb_saisie_grist qui les confondrait
tous, une colonne nb_<gestionnaire> est créée par organisme réellement vu
dans les signalements actifs (ex: nb_redon_agglomeration), à la volée via
l'API Grist si elle n'existe pas encore. Les autres sources (DIRO, CD35,
CD44, CD56, Rennes Métropole) restent sur leur colonne fixe : ce sont chacune
un seul organisme, pas de découpage nécessaire. CD35 Inondations Linéaire
partage la colonne nb_cd35 avec CD35 Inondations (ponctuel) : même
gestionnaire CD35, décompte agrégé plutôt qu'une colonne par géométrie.
"""

import collections
import json
import os
import re
import unicodedata
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

# ================================================================================
# CONFIGURATION
# ================================================================================

# data/github/ (pas data/gitlab/) : copie de ce dépôt, lit ce que le job
# update-geojson de CE pipeline vient de publier, pas la sortie GitLab -
# sinon aucune vraie redondance si gitlab-forge tombe (on lirait une
# sortie GitLab plus jamais rafraîchie plutôt que la nôtre).
SIGNALEMENTS_URL = "https://geobretagne.fr/apps/routes_coupees_inondation/data/github/signalements.geojson"
MASQUE_URL = "https://geobretagne.fr/apps/routes_coupees_inondation/masque.geojson"

GRIST_API_KEY = os.environ.get("GRIST_API_KEY")
GRIST_STATS_DOC_ID = os.environ.get("GRIST_STATS_DOC_ID")
GRIST_API_BASE = "https://grist.dataregion.fr/o/inforoute/api"
TABLE_SITUATION = "Situation_actuelle"
TABLE_HISTORIQUE_SIGNALEMENTS = "Historique_signalements"

HTTP_TIMEOUT = 30

SAISIE_GRIST_SOURCE = "Saisie Grist"

# properties.source (cf merge_signalements.py) → colonne Grist fixe.
# Saisie Grist n'y figure pas : sa colonne est dynamique, cf
# _gestionnaire_column() plus bas.
SOURCE_COLUMNS = {
    "DIRO": "nb_diro",
    "Rennes Métropole": "nb_rennes_metropole",
    "CD35 Inondations": "nb_cd35",
    "CD35 Inondations Linéaire": "nb_cd35",
    "CD44": "nb_cd44",
    "CD56": "nb_cd56",
}

# colonne fixe → libellé affiché dans la page HTML (mêmes noms que dans
# Grist, raccourcis pour l'affichage).
SOURCE_DISPLAY = {
    "nb_diro": "DIRO",
    "nb_rennes_metropole": "Rennes Métropole",
    "nb_cd35": "CD35",
    "nb_cd44": "CD44",
    "nb_cd56": "CD56",
}

HTML_OUTPUT_FILE = "stats-prefecture.html"


# ================================================================================
# COLONNE DYNAMIQUE PAR GESTIONNAIRE (Saisie Grist)
# ================================================================================


def _slugify(text):
    """'Redon Agglomération' -> 'redon_agglomeration' : identifiant de
    colonne Grist valide (sans accent ni espace)."""
    text = unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-zA-Z0-9]+", "_", text).strip("_").lower()
    return text or "autre"


def _gestionnaire_column(gestionnaire):
    return "nb_" + _slugify(gestionnaire)


def discover_gestionnaire_columns(active_features):
    """{colonne: libellé} pour chaque gestionnaire Saisie Grist rencontré
    dans ce run, dans l'ordre de première apparition."""
    columns = collections.OrderedDict()
    for feature in active_features:
        props = feature.get("properties") or {}
        if props.get("source") != SAISIE_GRIST_SOURCE:
            continue
        gestionnaire = props.get("gestionnaire") or "Autre"
        columns.setdefault(_gestionnaire_column(gestionnaire), gestionnaire)
    return columns


def _feature_column(props):
    source = props.get("source") or ""
    if source == SAISIE_GRIST_SOURCE:
        return _gestionnaire_column(props.get("gestionnaire") or "Autre")
    return SOURCE_COLUMNS.get(source)


# ================================================================================
# PÉRIMÈTRE (masque.geojson, même fichier et même règle que le visualiseur)
# ================================================================================


def fetch_masque_rings():
    """Anneaux (extérieur + trous) du MultiPolygon masque.geojson. Le masque
    couvre le monde entier avec le vrai périmètre découpé en trou dedans —
    donc un point DANS ce trou est HORS des anneaux du masque."""
    resp = requests.get(MASQUE_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    features = resp.json().get("features") or []
    rings = []
    for feature in features:
        geometry = feature.get("geometry") or {}
        if geometry.get("type") != "MultiPolygon":
            continue
        for polygon in geometry.get("coordinates") or []:
            rings.extend(polygon)
    return rings


def _point_in_ring(x, y, ring):
    """Ray casting standard (PNPOLY)."""
    inside = False
    n = len(ring)
    j = n - 1
    for i in range(n):
        xi, yi = ring[i]
        xj, yj = ring[j]
        if (yi > y) != (yj > y) and x < (xj - xi) * (y - yi) / (yj - yi) + xi:
            inside = not inside
        j = i
    return inside


def _in_masque(point, rings):
    """Même règle que OpenLayers intersectsCoordinate() sur un polygone à
    trous : un point est dans la forme s'il est dans un nombre impair
    d'anneaux (l'anneau extérieur le compte dedans, un trou l'en ressort)."""
    x, y = point
    inside = False
    for ring in rings:
        if _point_in_ring(x, y, ring):
            inside = not inside
    return inside


def _representative_point(geometry):
    """Même règle que getGeometryCenter() côté visualiseur : le point du
    milieu (index entier, pas d'interpolation) pour une ligne, le 1er
    sous-tracé pour une MultiLineString."""
    geom_type = geometry.get("type")
    coords = geometry.get("coordinates")
    if geom_type == "Point":
        return coords
    if geom_type == "LineString" and coords:
        return coords[len(coords) // 2]
    if geom_type == "MultiLineString" and coords and coords[0]:
        line = coords[0]
        return line[len(line) // 2]
    return None


def is_in_perimetre(feature, masque_rings):
    """Hors périmètre par défaut si la géométrie est absente/inattendue."""
    point = _representative_point(feature.get("geometry") or {})
    if point is None:
        return False
    return not _in_masque(point, masque_rings)


# ================================================================================
# LECTURE DES SIGNALEMENTS
# ================================================================================


def fetch_signalements():
    resp = requests.get(SIGNALEMENTS_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    features = resp.json().get("features") or []
    print(f"{len(features)} signalement(s) actif(s) récupéré(s)")
    return features


# ================================================================================
# CLASSIFICATION ET AGRÉGATION PAR COMMUNE
# ================================================================================


_RD_RE = re.compile(r"^R?D\s?\d")
_RN_RE = re.compile(r"^R?N\s?\d")


def _classe_route(route, source):
    """RD/RN/Autres d'après le nom réel de la route ('D256' ou 'RD256' selon
    la source). DIRO reste un filet de repli pour une route nationale sans
    nom renseigné."""
    route = (route or "").strip()
    if _RD_RE.match(route):
        return "RD"
    if _RN_RE.match(route) or source == "DIRO":
        return "RN"
    return "Autres"


def filter_active_features(features, masque_rings):
    """Sépare les signalements du périmètre (masque.geojson) en deux : actifs
    (statut_actif=True) et résolus depuis moins de 3 jours (statut_actif=
    False, encore dans signalements.geojson pour l'affichage "réouverture
    récente" côté viewer, cf should_keep_feature() dans merge_signalements.py).
    Les actifs alimentent l'agrégat par commune ; les deux alimentent
    l'historique par signalement (sync_historique_signalements) - les résolus
    y servent à récupérer le date_fin déclaré par la source au moment de la
    fermeture, avant qu'ils ne disparaissent complètement du flux."""
    active = []
    resolved = []
    hors_perimetre = 0

    for feature in features:
        props = feature.get("properties") or {}
        if not is_in_perimetre(feature, masque_rings):
            hors_perimetre += 1
            continue
        if props.get("statut_actif") is True:
            active.append(feature)
        else:
            resolved.append(feature)

    if resolved:
        print(f"{len(resolved)} signalement(s) résolu(s) (réouverture récente) exclu(s) des stats actives")
    if hors_perimetre:
        print(f"{hors_perimetre} signalement(s) hors périmètre (masque.geojson) ignoré(s)")

    return active, resolved


def aggregate_by_commune(active_features, count_columns):
    """Une coupure compte dans CHAQUE commune que sa géométrie traverse
    (properties.codes_insee, déjà dédupliqué par enrich_with_communes() dans
    merge_signalements.py) - sinon une commune réellement touchée par un
    tronçon linéaire mais qui n'est pas le point de départ n'apparaissait
    jamais dans la table. Le total global, lui, ne compte qu'une fois par
    événement (_add_to_row(total, ...) hors de la boucle sur codes) pour
    rester le nombre réel de coupures, pas la somme des lignes par commune.

    Labellisation : uniquement via properties.noms_communes (notre propre
    géocodage inverse, même index que codes_insee - PAS properties.commune,
    le texte libre déclaré par la source pour l'ensemble du signalement).
    Bug constaté le 02/09 en associant ce texte au 1er code : rien ne garantit
    que le 1er point échantillonné (cf _sample_points_for_commune_lookup)
    corresponde à la commune que la source a en tête en le renseignant - sur
    un tronçon Mordelles → Bréal-sous-Montfort déclaré "Bréal-sous-Montfort",
    le 1er point tombait à Mordelles (35196), qui s'est donc retrouvée
    étiquetée "Bréal-sous-Montfort" par erreur alors que 35037 (la vraie
    Bréal-sous-Montfort) portait déjà ce nom via son propre géocodage - deux
    lignes identiques en apparence pour deux communes différentes.

    count_columns : colonnes fixes (SOURCE_COLUMNS) + colonnes dynamiques par
    gestionnaire découvertes dans ce run (discover_gestionnaire_columns) —
    chaque ligne les initialise toutes à 0 pour que la table Grist affiche un
    vrai zéro plutôt qu'une case vide pour une commune non concernée."""
    par_commune = collections.OrderedDict()
    commune_labels = {}
    total = _new_row(count_columns)

    for feature in active_features:
        props = feature.get("properties") or {}
        codes = props.get("codes_insee") or []
        noms = props.get("noms_communes") or []
        classe = _classe_route(props.get("route") or "", props.get("source") or "")
        colname = _feature_column(props)

        _add_to_row(total, classe, colname)

        for i, code in enumerate(codes):
            row = par_commune.setdefault(code, _new_row(count_columns))
            _add_to_row(row, classe, colname)
            if code not in commune_labels and i < len(noms) and noms[i]:
                commune_labels[code] = noms[i]

    return par_commune, commune_labels, total


def _new_row(count_columns):
    row = {"nb_coupures": 0, "nb_rn": 0, "nb_rd": 0, "nb_autres": 0}
    for colname in count_columns:
        row[colname] = 0
    return row


def _add_to_row(row, classe, colname):
    row["nb_coupures"] += 1
    row["nb_" + classe.lower()] += 1
    if colname:
        row[colname] += 1


def build_rows(par_commune, commune_labels, total):
    rows = []
    for code, row in sorted(par_commune.items()):
        nom = commune_labels.get(code, code)
        rows.append({"code_insee": code, "commune": nom, **row})
    rows.append({"code_insee": "Total", "commune": "Total", **total})
    return rows


# ================================================================================
# PAGE HTML (récapitulatif visuel, publiée à côté de signalements.geojson)
# ================================================================================


def _js(obj):
    """JSON pour un <script> : échappe '</' pour ne pas fermer la balise si
    jamais une valeur (nom de route saisi à la main...) contenait ce texte."""
    return json.dumps(obj, ensure_ascii=False).replace("</", "<\\/")


def build_html(par_commune, commune_labels, total, generated_label, gestionnaire_columns):
    rows = [{"code_insee": code, "commune": commune_labels.get(code, code), **row} for code, row in par_commune.items()]
    rows.sort(key=lambda r: -r["nb_coupures"])

    # Colonnes du tableau détaillé : fixes + une par gestionnaire découvert
    # dans ce run (ex: nb_redon_agglomeration) — remplace l'ancienne colonne
    # unique "Grist" qui mélangeait tous les organismes saisissant via Grist.
    table_columns = [
        ("nb_coupures", "Total"),
        ("nb_rn", "RN"),
        ("nb_rd", "RD"),
        ("nb_autres", "Autres"),
        ("nb_diro", "DIRO"),
        ("nb_rennes_metropole", "Rennes Métro."),
        ("nb_cd35", "CD35"),
        ("nb_cd44", "CD44"),
        ("nb_cd56", "CD56"),
    ] + list(gestionnaire_columns.items())

    # Répartition par organisme (graphique) : une couleur par source fixe +
    # UNE couleur agrégée pour "Saisie Grist", plutôt qu'une couleur par
    # gestionnaire — la liste des gestionnaires est appelée à s'allonger,
    # une teinte générée par organisme n'aurait pas de sens dans un
    # graphique (cf compétence dataviz : jamais de teinte catégorielle
    # générée à la volée). Le détail par organisme reste dans le tableau.
    organisme_chart = [{"label": "DIRO", "val": total.get("nb_diro", 0)}]
    organisme_chart.append({"label": "Saisie Grist", "val": sum(total.get(col, 0) for col in gestionnaire_columns)})
    organisme_chart += [
        {"label": label, "val": total.get(col, 0)}
        for col, label in SOURCE_DISPLAY.items()
        if col != "nb_diro"
    ]

    return HTML_TEMPLATE.format(
        generated_label=generated_label,
        rows_json=_js(rows),
        total_json=_js(total),
        table_columns_json=_js(table_columns),
        organisme_chart_json=_js(organisme_chart),
    )


HTML_TEMPLATE = """<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<title>Coupures de routes — Vilaine et affluents</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root {{
    color-scheme: light;
    --brand: #000091;
    --brand-ink: #ffffff;
    --page: #f4f5f9;
    --surface: #ffffff;
    --surface-2: #eceef5;
    --ink: #10131c;
    --ink-2: #4b5165;
    --ink-muted: #868da3;
    --grid: #e3e6ee;
    --baseline: #c7cbdb;
    --border: rgba(16, 19, 28, 0.10);
    --live-bg: #e6f4ea;
    --live-ink: #1c6b34;
    --live-border: #a9d8b7;

    --s1: #2a78d6;
    --s2: #eb6834;
    --s3: #1baf7a;
    --s4: #eda100;
    --s5: #e87ba4;
    --s6: #008300;
  }}

  @media (prefers-color-scheme: dark) {{
    :root:where(:not([data-theme="light"])) {{
      color-scheme: dark;
      --brand: #9a9aff;
      --brand-ink: #10101f;
      --page: #0d0f16;
      --surface: #171a24;
      --surface-2: #1f2330;
      --ink: #f5f6fa;
      --ink-2: #b7bccb;
      --ink-muted: #7f8598;
      --grid: #2a2e3c;
      --baseline: #383f52;
      --border: rgba(255, 255, 255, 0.10);
      --live-bg: #123321;
      --live-ink: #7fd89a;
      --live-border: #245c38;

      --s1: #3987e5;
      --s2: #d95926;
      --s3: #199e70;
      --s4: #c98500;
      --s5: #d55181;
      --s6: #2fa62f;
    }}
  }}
  :root[data-theme="dark"] {{
    color-scheme: dark;
    --brand: #9a9aff;
    --brand-ink: #10101f;
    --page: #0d0f16;
    --surface: #171a24;
    --surface-2: #1f2330;
    --ink: #f5f6fa;
    --ink-2: #b7bccb;
    --ink-muted: #7f8598;
    --grid: #2a2e3c;
    --baseline: #383f52;
    --border: rgba(255, 255, 255, 0.10);
    --live-bg: #123321;
    --live-ink: #7fd89a;
    --live-border: #245c38;

    --s1: #3987e5;
    --s2: #d95926;
    --s3: #199e70;
    --s4: #c98500;
    --s5: #d55181;
    --s6: #2fa62f;
  }}
  :root[data-theme="light"] {{
    color-scheme: light;
    --brand: #000091;
    --brand-ink: #ffffff;
    --page: #f4f5f9;
    --surface: #ffffff;
    --surface-2: #eceef5;
    --ink: #10131c;
    --ink-2: #4b5165;
    --ink-muted: #868da3;
    --grid: #e3e6ee;
    --baseline: #c7cbdb;
    --border: rgba(16, 19, 28, 0.10);
    --live-bg: #e6f4ea;
    --live-ink: #1c6b34;
    --live-border: #a9d8b7;

    --s1: #2a78d6;
    --s2: #eb6834;
    --s3: #1baf7a;
    --s4: #eda100;
    --s5: #e87ba4;
    --s6: #008300;
  }}

  * {{ box-sizing: border-box; }}
  html, body {{
    margin: 0;
    background: var(--page);
    color: var(--ink);
    font-family: "Segoe UI", system-ui, -apple-system, "Helvetica Neue", Arial, sans-serif;
  }}
  body {{ -webkit-font-smoothing: antialiased; }}

  .page {{
    max-width: 1040px;
    margin: 0 auto;
    padding: 28px 24px 64px;
    display: flex;
    flex-direction: column;
    gap: 28px;
  }}

  .masthead {{
    background: var(--brand);
    color: var(--brand-ink);
    border-radius: 10px;
    padding: 28px 32px;
    display: flex;
    flex-direction: column;
    gap: 10px;
  }}
  .eyebrow {{
    font-size: 12px;
    font-weight: 800;
    letter-spacing: 0.11em;
    text-transform: uppercase;
    opacity: 0.82;
  }}
  .masthead h1 {{
    margin: 0;
    font-size: clamp(24px, 3.4vw, 34px);
    font-weight: 800;
    letter-spacing: -0.01em;
    text-wrap: balance;
    font-family: "Avenir Next", "Century Gothic", "Segoe UI Semibold", "Segoe UI", sans-serif;
  }}
  .masthead-meta {{
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 12px;
    margin-top: 4px;
  }}
  .masthead-date {{ font-size: 14px; opacity: 0.88; }}
  .badge-live {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-size: 12px;
    font-weight: 700;
    letter-spacing: 0.02em;
    padding: 5px 11px;
    border-radius: 999px;
    background: var(--live-bg);
    color: var(--live-ink);
    border: 1px solid var(--live-border);
  }}
  .badge-live::before {{ content: "●"; font-size: 8px; }}

  .kpi-row {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 12px;
  }}
  .kpi-tile {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 16px 18px;
    display: flex;
    flex-direction: column;
    gap: 6px;
  }}
  .kpi-label {{
    font-size: 12px;
    font-weight: 600;
    color: var(--ink-muted);
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }}
  .kpi-value {{ font-size: 30px; font-weight: 800; line-height: 1; color: var(--ink); }}
  .kpi-tile.accent .kpi-value {{ color: var(--brand); }}
  .kpi-sub {{ font-size: 12.5px; color: var(--ink-2); }}

  .panel {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 20px 22px 22px;
  }}
  .panel h2 {{ margin: 0 0 3px; font-size: 16px; font-weight: 800; letter-spacing: -0.005em; }}
  .panel .panel-sub {{ margin: 0 0 18px; font-size: 12.5px; color: var(--ink-muted); }}

  .two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
  @media (max-width: 720px) {{ .two-col {{ grid-template-columns: 1fr; }} }}

  .bar-chart {{ display: flex; flex-direction: column; gap: 9px; }}
  .bar-row {{ display: grid; grid-template-columns: 156px 1fr; align-items: center; gap: 10px; }}
  .bar-name {{
    font-size: 13px; color: var(--ink-2); text-align: right;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  }}
  .bar-track {{ display: flex; align-items: center; gap: 8px; height: 22px; }}
  .bar-fill {{ height: 20px; background: var(--brand); border-radius: 0 4px 4px 0; min-width: 3px; flex-shrink: 0; }}
  .bar-val {{ font-size: 12.5px; font-weight: 700; color: var(--ink); font-variant-numeric: tabular-nums; }}
  .bar-note {{ margin-top: 6px; font-size: 12px; color: var(--ink-muted); padding-left: 166px; }}
  .table-note {{ margin-top: 10px; font-size: 12px; color: var(--ink-muted); }}

  .stack {{ display: flex; gap: 2px; height: 30px; border-radius: 6px; overflow: hidden; background: var(--surface); }}
  .stack-seg {{ min-width: 4px; display: flex; align-items: center; justify-content: center; }}
  .stack-seg .seg-label {{ font-size: 11.5px; font-weight: 700; color: #fff; text-shadow: 0 1px 1px rgba(0,0,0,0.18); white-space: nowrap; }}

  .legend {{ display: flex; flex-wrap: wrap; gap: 8px 18px; margin-top: 16px; }}
  .legend-item {{ display: flex; align-items: baseline; gap: 7px; font-size: 12.5px; color: var(--ink-2); }}
  .legend-swatch {{ width: 10px; height: 10px; border-radius: 3px; display: inline-block; flex-shrink: 0; transform: translateY(1px); }}
  .legend-label {{ color: var(--ink); font-weight: 600; }}
  .legend-val {{ font-variant-numeric: tabular-nums; color: var(--ink-muted); }}

  .table-scroll {{ overflow-x: auto; border: 1px solid var(--border); border-radius: 10px; }}
  table {{ border-collapse: collapse; width: 100%; min-width: 760px; font-size: 13px; }}
  thead th {{
    position: sticky; top: 0; background: var(--surface-2); color: var(--ink-2);
    font-weight: 700; text-align: right; padding: 9px 12px; border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }}
  thead th:first-child, thead th:nth-child(2) {{ text-align: left; }}
  tbody td {{
    padding: 7px 12px; text-align: right; border-bottom: 1px solid var(--grid);
    font-variant-numeric: tabular-nums; color: var(--ink-2);
  }}
  tbody td:first-child {{ color: var(--ink-muted); }}
  tbody td:first-child, tbody td:nth-child(2) {{ text-align: left; font-variant-numeric: normal; }}
  tbody td:nth-child(2) {{ color: var(--ink); font-weight: 600; }}
  tbody tr.total-row td {{
    font-weight: 800; color: var(--ink); border-top: 2px solid var(--baseline); border-bottom: none; background: var(--surface-2);
  }}

  .empty-panel {{
    background: var(--surface); border: 1px solid var(--border); border-radius: 10px;
    padding: 48px 24px; text-align: center; color: var(--ink-2); font-size: 14px;
  }}

  footer.note {{
    font-size: 12.5px; color: var(--ink-muted); line-height: 1.6;
    border-top: 1px solid var(--border); padding-top: 16px;
  }}
</style>
</head>
<body>
<div class="page">

  <div class="masthead">
    <div class="eyebrow">Vilaine et affluents</div>
    <h1>Coupures de routes liées aux inondations</h1>
    <div class="masthead-meta">
      <span class="masthead-date">Situation au {generated_label}</span>
      <span class="badge-live">Mis à jour automatiquement</span>
    </div>
  </div>

  <div id="content"></div>

  <footer class="note">
    Page générée automatiquement à partir de signalements.geojson (coupures actives, bassin de la Vilaine et affluents).
    Le détail est aussi disponible dans le document Grist « Statistiques préfecture ».
  </footer>

</div>

<script>
  const ROWS = {rows_json};
  const TOTAL = {total_json};
  const TABLE_COLUMNS = {table_columns_json};
  const ORGANISME_CHART = {organisme_chart_json};
  const STACK_COLORS = ["var(--s1)", "var(--s2)", "var(--s3)", "var(--s4)", "var(--s5)", "var(--s6)"];

  const content = document.getElementById("content");

  if (TOTAL.nb_coupures === 0) {{
    content.innerHTML = `<div class="empty-panel">Aucune coupure actuellement recensée dans le bassin de la Vilaine et affluents.</div>`;
  }} else {{
    content.innerHTML = `
      <div class="kpi-row">
        <div class="kpi-tile accent">
          <div class="kpi-label">Coupures actives</div>
          <div class="kpi-value">${{TOTAL.nb_coupures}}</div>
          <div class="kpi-sub">sur ${{ROWS.length}} commune${{ROWS.length > 1 ? "s" : ""}}</div>
        </div>
        <div class="kpi-tile">
          <div class="kpi-label">Routes départementales</div>
          <div class="kpi-value">${{TOTAL.nb_rd}}</div>
          <div class="kpi-sub">${{pct(TOTAL.nb_rd)}}&nbsp;% des coupures</div>
        </div>
        <div class="kpi-tile">
          <div class="kpi-label">Routes nationales</div>
          <div class="kpi-value">${{TOTAL.nb_rn}}</div>
          <div class="kpi-sub">${{pct(TOTAL.nb_rn)}}&nbsp;% des coupures</div>
        </div>
        <div class="kpi-tile">
          <div class="kpi-label">Autres voies</div>
          <div class="kpi-value">${{TOTAL.nb_autres}}</div>
          <div class="kpi-sub">${{pct(TOTAL.nb_autres)}}&nbsp;% des coupures</div>
        </div>
      </div>

      <div class="panel">
        <h2>Coupures par commune</h2>
        <p class="panel-sub" id="bar-sub"></p>
        <div class="bar-chart" id="bar-chart"></div>
        <p class="bar-note" id="bar-note" style="display:none"></p>
      </div>

      <div class="two-col">
        <div class="panel">
          <h2>Répartition par type de route</h2>
          <p class="panel-sub">part du total des coupures</p>
          <div class="stack" id="stack-route"></div>
          <div class="legend" id="legend-route"></div>
        </div>
        <div class="panel">
          <h2>Répartition par organisme</h2>
          <p class="panel-sub">source du signalement</p>
          <div class="stack" id="stack-source"></div>
          <div class="legend" id="legend-source"></div>
        </div>
      </div>

      <div class="panel">
        <h2>Détail par commune</h2>
        <p class="panel-sub">${{ROWS.length}} commune${{ROWS.length > 1 ? "s" : ""}} touchée${{ROWS.length > 1 ? "s" : ""}}, bassin de la Vilaine et affluents</p>
        <div class="table-scroll">
          <table id="detail-table">
            <thead>
              <tr id="detail-thead-row"></tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
        <p class="table-note">Une coupure qui traverse plusieurs communes compte dans chacune d'elles : la somme des lignes peut dépasser le total de coupures actives ci-dessus.</p>
      </div>
    `;

    function pct(val) {{
      return (TOTAL.nb_coupures ? (val / TOTAL.nb_coupures) * 100 : 0).toFixed(1).replace(".", ",");
    }}

    // ---------- Bar chart : communes ----------
    const top = ROWS.slice(0, 15);
    const maxVal = top[0].nb_coupures;
    document.getElementById("bar-sub").textContent =
      ROWS.length > 15 ? "15 communes les plus touchées" : "toutes les communes touchées";
    const barChart = document.getElementById("bar-chart");
    top.forEach((r) => {{
      const pctWidth = Math.max((r.nb_coupures / maxVal) * 100, 4);
      const row = document.createElement("div");
      row.className = "bar-row";
      row.innerHTML = `
        <div class="bar-name" title="${{r.commune}}">${{r.commune}}</div>
        <div class="bar-track">
          <div class="bar-fill" style="width:${{pctWidth}}%"></div>
          <span class="bar-val">${{r.nb_coupures}}</span>
        </div>`;
      barChart.appendChild(row);
    }});
    if (ROWS.length > 15) {{
      const note = document.getElementById("bar-note");
      note.style.display = "block";
      note.textContent = `+ ${{ROWS.length - 15}} autre(s) commune(s) — détail dans le tableau ci-dessous`;
    }}

    // ---------- Stacked bar : type de route ----------
    buildStack("stack-route", "legend-route", [
      {{ label: "Autres voies", val: TOTAL.nb_autres, color: "var(--s1)" }},
      {{ label: "Départementales", val: TOTAL.nb_rd, color: "var(--s2)" }},
      {{ label: "Nationales", val: TOTAL.nb_rn, color: "var(--s3)" }},
    ]);

    // ---------- Stacked bar : organisme ----------
    // Une couleur fixe par source institutionnelle + une couleur agrégée
    // pour "Saisie Grist" (le détail par organisme saisissant est dans le
    // tableau, cf build_html() côté Python).
    buildStack(
      "stack-source",
      "legend-source",
      ORGANISME_CHART.map((s, i) => ({{ ...s, color: STACK_COLORS[i] }}))
    );

    function buildStack(stackId, legendId, segs) {{
      const stackEl = document.getElementById(stackId);
      const legendEl = document.getElementById(legendId);
      segs.filter((s) => s.val > 0).forEach((s) => {{
        const p = (s.val / TOTAL.nb_coupures) * 100;
        const seg = document.createElement("div");
        seg.className = "stack-seg";
        seg.style.flexGrow = String(s.val);
        seg.style.background = s.color;
        if (p >= 9) seg.innerHTML = `<span class="seg-label">${{p.toFixed(0)}}%</span>`;
        seg.title = `${{s.label}} — ${{s.val}} (${{p.toFixed(1)}}%)`;
        stackEl.appendChild(seg);

        const item = document.createElement("div");
        item.className = "legend-item";
        item.innerHTML = `
          <span class="legend-swatch" style="background:${{s.color}}"></span>
          <span class="legend-label">${{s.label}}</span>
          <span class="legend-val">${{s.val}} · ${{p.toFixed(1).replace(".", ",")}}%</span>`;
        legendEl.appendChild(item);
      }});
    }}

    // ---------- Table ----------
    // En-tête et colonnes de comptage pilotées par TABLE_COLUMNS (fixes +
    // une par gestionnaire découvert ce run) plutôt que codées en dur, pour
    // s'adapter sans changement de code quand un nouvel organisme saisit.
    document.getElementById("detail-thead-row").innerHTML =
      ["Code INSEE", "Commune", ...TABLE_COLUMNS.map(([, label]) => label)]
        .map((label) => `<th>${{label}}</th>`).join("");

    function rowCells(r) {{
      return [r.code_insee, r.commune, ...TABLE_COLUMNS.map(([col]) => r[col] ?? 0)]
        .map((v) => `<td>${{v}}</td>`).join("");
    }}

    const tbody = document.querySelector("#detail-table tbody");
    [...ROWS].sort((a, b) => a.code_insee.localeCompare(b.code_insee)).forEach((r) => {{
      const tr = document.createElement("tr");
      tr.innerHTML = rowCells(r);
      tbody.appendChild(tr);
    }});
    const totalRow = document.createElement("tr");
    totalRow.className = "total-row";
    totalRow.innerHTML = rowCells({{ code_insee: "Total", commune: "Total", ...TOTAL }});
    tbody.appendChild(totalRow);
  }}
</script>
</body>
</html>
"""


# ================================================================================
# ÉCRITURE DANS GRIST
# ================================================================================


def _grist_headers():
    return {"Authorization": f"Bearer {GRIST_API_KEY}", "Content-Type": "application/json"}


def _grist_table_url(table_id):
    return f"{GRIST_API_BASE}/docs/{GRIST_STATS_DOC_ID}/tables/{table_id}/records"


def _grist_columns_url(table_id):
    return f"{GRIST_API_BASE}/docs/{GRIST_STATS_DOC_ID}/tables/{table_id}/columns"


def ensure_gestionnaire_columns_exist(table_id, gestionnaire_columns):
    """Crée dans Grist les colonnes nb_<gestionnaire> qui n'existent pas
    encore (un nouvel organisme qui commence à saisir via Grist) — les
    colonnes fixes (nb_coupures, nb_diro...) restent créées à la main une
    fois pour toutes, seules les colonnes dynamiques par gestionnaire sont
    concernées ici."""
    if not gestionnaire_columns:
        return
    resp = requests.get(_grist_columns_url(table_id), headers=_grist_headers(), timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    existing = {c["id"] for c in resp.json().get("columns") or []}

    missing = {col: label for col, label in gestionnaire_columns.items() if col not in existing}
    if not missing:
        return

    resp = requests.post(
        _grist_columns_url(table_id),
        headers=_grist_headers(),
        json={
            "columns": [
                {"id": col, "fields": {"label": label, "type": "Int"}} for col, label in missing.items()
            ]
        },
        timeout=HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    for col, label in missing.items():
        print(f"[{table_id}] colonne créée : {col} ({label})")


def replace_table_contents(table_id, rows):
    """Vide la table puis y insère `rows`."""
    resp = requests.get(_grist_table_url(table_id), headers=_grist_headers(), timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    existing_ids = [r["id"] for r in resp.json().get("records") or []]

    if existing_ids:
        resp = requests.post(
            _grist_table_url(table_id) + "/delete",
            headers=_grist_headers(),
            json=existing_ids,
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()

    append_rows(table_id, rows)
    print(f"[{table_id}] {len(existing_ids)} ligne(s) remplacée(s) par {len(rows)}")


def append_rows(table_id, rows):
    resp = requests.post(
        _grist_table_url(table_id),
        headers=_grist_headers(),
        json={"records": [{"fields": row} for row in rows]},
        timeout=HTTP_TIMEOUT,
    )
    resp.raise_for_status()


def patch_rows(table_id, records):
    """records: [{"id": <id Grist>, "fields": {...}}, ...]."""
    resp = requests.patch(
        _grist_table_url(table_id),
        headers=_grist_headers(),
        json={"records": records},
        timeout=HTTP_TIMEOUT,
    )
    resp.raise_for_status()


def _normalize_source_date(date_string):
    """Convertit une date au format 'DD/MM/YYYY à HHhMM' (format produit par
    format_date() dans merge_signalements.py, celui de date_debut/date_fin)
    vers le même format que date_run ('YYYY-MM-DD HH:MM') - sans ça, la
    colonne Grist mélangerait deux formats différents selon que la date
    vienne de la source ou du run."""
    if not date_string:
        return None
    try:
        return datetime.strptime(date_string, "%d/%m/%Y à %Hh%M").strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return None


def _historique_key(fields):
    """(source, id_source) : seule paire stable d'un run à l'autre pour
    identifier le même signalement (properties.id de signalements.geojson
    n'est qu'un compteur ré-attribué à chaque run de merge_signalements.py,
    donc inutilisable ici)."""
    source = fields.get("source")
    id_source = fields.get("id_source")
    if not source or not id_source:
        return None
    return (source, str(id_source))


def fetch_open_historique_records(table_id):
    """Lignes de Historique_signalements pas encore refermées
    (date_reouverture vide), indexées par (source, id_source) -> {id Grist,
    route}. route est gardée pour détecter dans sync_historique_signalements
    qu'un id_source réutilisé pendant qu'il est encore ouvert correspond en
    fait à une coupure différente (ex: une ligne Grist rééditée par erreur
    sans être repassée par Statut=Résolu d'abord)."""
    resp = requests.get(_grist_table_url(table_id), headers=_grist_headers(), timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    open_records = {}
    for record in resp.json().get("records") or []:
        fields = record["fields"]
        if fields.get("date_reouverture"):
            continue
        key = _historique_key(fields)
        if key:
            open_records[key] = {"id": record["id"], "route": fields.get("route") or ""}
    return open_records


def sync_historique_signalements(table_id, active_features, resolved_features, date_run):
    """Historique par signalement (un point/tronçon = une ligne), pas par
    commune : compare les signalements actifs du run courant aux lignes
    encore ouvertes dans Historique_signalements. Une nouvelle coupure
    (source+id_source jamais vue ouverte) crée une ligne ; une coupure
    ouverte qui n'apparaît plus dans le run courant (réouverture) complète
    sa ligne existante avec date_reouverture, plutôt que d'en recréer une.

    date_apparition/date_reouverture préfèrent la date déclarée par la
    source (date_debut/date_fin) quand elle existe, et ne retombent sur
    l'heure du run (date_run) que si la source ne fournit rien - depuis fin
    08/2026 c'est le cas de CD35 quand Date_ouverture est renseigné (cf
    _cd35_properties() dans merge_signalements.py), mais le repli reste
    nécessaire : CD35 peut toujours retirer une entrée de son flux sans
    jamais renseigner ce champ (cf objectif.md §4.2)."""
    current = {}
    ignores = 0
    for feature in active_features:
        props = feature.get("properties") or {}
        key = _historique_key(props)
        if key is None:
            ignores += 1
            continue
        current[key] = props
    if ignores:
        print(f"{ignores} signalement(s) sans id_source ignoré(s) pour l'historique")

    resolved_by_key = {}
    for feature in resolved_features:
        props = feature.get("properties") or {}
        key = _historique_key(props)
        if key:
            resolved_by_key[key] = props

    open_records = fetch_open_historique_records(table_id)

    new_rows = []
    closed_updates = []
    reused_ids = 0
    for key, props in current.items():
        open_record = open_records.get(key)
        if open_record is not None:
            # id_source déjà ouvert : normalement le même épisode continue,
            # rien à faire. Sauf si route a changé - la source a réutilisé le
            # même id_source pour une coupure différente sans jamais
            # repasser par une résolution explicite (ex: ligne Grist rééditée
            # au lieu d'être close puis recréée). On referme alors l'ancienne
            # ligne à l'instant où on le remarque (date_run, pas de date
            # "réelle" disponible pour un changement qu'on n'a pas vu venir)
            # et on laisse le new_rows du dessous ouvrir la nouvelle.
            if (props.get("route") or "") == open_record["route"]:
                continue
            closed_updates.append({"id": open_record["id"], "fields": {"date_reouverture": date_run}})
            reused_ids += 1

        source, id_source = key
        codes = props.get("codes_insee") or []
        new_rows.append(
            {
                "source": source,
                "gestionnaire": props.get("gestionnaire") or "",
                "id_source": id_source,
                "route": props.get("route") or "",
                "commune": props.get("commune") or "",
                "code_insee": ", ".join(codes) if codes else "",
                "cause": props.get("cause") or "",
                "type_coupure": props.get("type_coupure") or "",
                "date_apparition": _normalize_source_date(props.get("date_debut")) or date_run,
                "date_reouverture": "",
            }
        )
    if new_rows:
        append_rows(table_id, new_rows)

    for key, open_record in open_records.items():
        if key in current:
            continue
        resolved_props = resolved_by_key.get(key) or {}
        date_reouverture = _normalize_source_date(resolved_props.get("date_fin")) or date_run
        closed_updates.append({"id": open_record["id"], "fields": {"date_reouverture": date_reouverture}})
    if closed_updates:
        patch_rows(table_id, closed_updates)

    print(f"[{table_id}] {len(new_rows)} nouvelle(s) coupure(s), {len(closed_updates)} réouverture(s) détectée(s)" + (f", {reused_ids} id_source réutilisé(s) en route" if reused_ids else ""))


# ================================================================================
# POINT D'ENTRÉE
# ================================================================================


def main():
    if not GRIST_API_KEY or not GRIST_STATS_DOC_ID:
        raise SystemExit("GRIST_API_KEY / GRIST_STATS_DOC_ID manquant(s)")

    print("Statistiques préfecture - Démarrage...\n")

    now_paris = datetime.now(ZoneInfo("Europe/Paris"))
    date_run = now_paris.strftime("%Y-%m-%d %H:%M")

    features = fetch_signalements()
    masque_rings = fetch_masque_rings()
    active_features, resolved_features = filter_active_features(features, masque_rings)

    gestionnaire_columns = discover_gestionnaire_columns(active_features)
    ensure_gestionnaire_columns_exist(TABLE_SITUATION, gestionnaire_columns)
    count_columns = list(SOURCE_COLUMNS.values()) + list(gestionnaire_columns.keys())

    par_commune, commune_labels, total = aggregate_by_commune(active_features, count_columns)
    rows = build_rows(par_commune, commune_labels, total)

    print(f"\n{len(rows) - 1} commune(s) touchée(s), {total['nb_coupures']} coupure(s) au total")

    replace_table_contents(TABLE_SITUATION, rows)
    sync_historique_signalements(TABLE_HISTORIQUE_SIGNALEMENTS, active_features, resolved_features, date_run)

    generated_label = now_paris.strftime("%d/%m/%Y à %Hh%M")
    html = build_html(par_commune, commune_labels, total, generated_label, gestionnaire_columns)
    with open(HTML_OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\nFichier {HTML_OUTPUT_FILE} créé")

    print("\nStatistiques préfecture - Terminé.")


if __name__ == "__main__":
    main()
