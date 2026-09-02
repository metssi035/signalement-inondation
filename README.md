# Système de signalement et de visualisation des routes coupées en cas d'inondation

Plateforme de collecte GRIST et de visualisation cartographique mviewer sur Geobretagne des perturbations routières en cas d'inondation en Bretagne (périmètre élargi : 35, 44, 56).

---

## Présentation

Système de collecte Grist, de moissonnage (API/flux) et de visualisation des routes coupées (mviewer) en cas d'inondations sur le réseau routier.

Le projet combine 7 sources de données complémentaires :
- Saisie manuelle par les agents (Redon Agglomération, autres) via un formulaire Grist
- DATEX II (Bison Futé) pour la DIRO
- API OGC pour le CD35 (signalements ponctuels)
- API OGC pour le CD35 (tronçons linéaires)
- API pour Rennes Métropole (WFS)
- API pour le CD44
- API OGC pour le CD56

Toutes ces données sont fusionnées, enrichies (commune INSEE par géocodage inverse) puis publiées sur GéoBretagne, à la fois pour la carte interactive (mviewer) et sous forme de fichiers ouverts (GeoJSON, CSV, Datex II XML).

---

## Objectifs

Le système vise à :
- Centraliser les routes coupées à cause des inondations, provenant de sources multiples
- Faciliter la coordination entre gestionnaires de voirie et préfecture
- Informer rapidement les usagers de l'état du réseau routier en cas de crise inondation
- Archiver l'historique des événements pour analyse

---

## Fonctionnement général

```
┌──────────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
│ Agents       │  │ DIRO     │  │ CD35     │  │ CD35     │  │ Rennes   │  │ CD44     │  │ CD56     │
│ (Grist)      │  │ (Datex2) │  │ (point)  │  │ (linéaire│  │ Métropole│  │ (API)    │  │ (OGC)    │
└──────┬───────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘
       └───────────────┴─────────────┴─────────────┴─────────────┴─────────────┴─────────────┘
                                             │
                                             ▼
                          scripts/merge_signalements.py  (GitLab CI, Python)
                          fusion + archivage annuel + enrichissement commune
                                             │
                    ┌────────────────────────┼────────────────────────┐
                    ▼                        ▼                        ▼
           signalements.geojson    signalements_inondation   signalements_inondation
           (carte mviewer)           .xml (Datex II)              .csv (grand public)
                    │
                    ▼
          scripts/stats_prefecture.py (GitLab CI, indépendant)
          agrégation par commune → doc Grist "Statistiques préfecture" + page HTML publique
```

En parallèle, deux robots indépendants alimentent le visualiseur et les statistiques :
- `fetch_vigicrues.py` : niveau de vigilance crues (bassin Vilaine), affiché dans le sous-titre du visualiseur.
- `fetch_stations_hydro.py` : hauteurs d'eau des stations Hub'Eau (35/44/56), en vue d'un futur croisement avec les coupures.

---

## Fichiers publiés (GéoBretagne, `data/gitlab/`)

| Fichier | Contenu |
|---|---|
| `signalements.geojson` | Toutes les coupures actives fusionnées (source du visualiseur) |
| `signalements_inondation.xml` | Export Datex II v2.3 des coupures pour cause d'inondation |
| `signalements_inondation.csv` | Export CSV grand public (toutes causes) |
| `metadata.json` | Statistiques + état de santé (monitoring) de chaque flux source |
| `archives/signalements_<année>.geojson` | Historique annuel par signalement |
| `stats-prefecture.html` | Récapitulatif par commune (RN/RD/Autres, par organisme) |
| `vigilance.json` | Niveau de vigilance crues courant |
| `stations_hydro.geojson` / `hauteurs_eau_<année>.geojson` | Stations Hub'Eau, instantané / accumulation |

---

## Scripts (`scripts/`)

| Script | Rôle | Tourne en CI ? |
|---|---|---|
| `merge_signalements.py` | Fusion des 7 sources, archivage, exports GeoJSON/XML/CSV | Oui, job `update-geojson`, planning `chain` |
| `fetch_datex_diro.py` | Récupération DIRO (Datex II Bison Futé) | Oui, job `fetch-datex-diro`, planning `chain` |
| `stats_prefecture.py` | Agrégation par commune → doc Grist + HTML | Oui, job `stats-prefecture`, planning `chain` |
| `fetch_vigicrues.py` | Niveau de vigilance crues (bassin Vilaine) | Oui, job `fetch-vigicrues`, planning `vigicrues` |
| `fetch_stations_hydro.py` | Stations Hub'Eau + hauteurs d'eau | Oui, job `fetch-stations-hydro`, planning `stations-hydro` |
| `import_hauteurs_hydroportail.py` | Import ponctuel d'un historique HydroPortail | Non, à lancer à la main, une fois |
| `reconstitution_coupures_historique.py` | Reconstitution ponctuelle de l'historique jan-fév 2026 | Non, à lancer à la main, une fois |

Le détail des jobs, plannings et variables CI/CD requises est documenté en en-tête de [`.gitlab-ci.yml`](.gitlab-ci.yml).

---

## Visualisation cartographique

**Accès au visualiseur** : [Carte interactive mviewer](https://geobretagne.fr/app/routes_coupees_inondation)

---

## Licence

Ce projet est distribué sous licence MIT. Voir le fichier [LICENSE](LICENSE) pour plus de détails.
