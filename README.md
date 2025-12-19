# Système de signalement et de visualisation des routes coupées en cas d'inondation

Plateforme de collecte GRIST et de visualisation cartographique mviewer sur Geobretagne des perturbations routières en cas d'inondation en Bretagne (périmètre élargi)

---

## Présentation

Système de collecte GRIST, de moissonnage (API/flux) et de visualisation des routes coupées (mviewer) en cas d'inondations sur le réseau routier.

Le projet combine plusieurs sources de données complémentaires :
- Moissonnage des données saisies dans Grist par les opérateurs (Redon agglomération, autres)
- Moissonnage automatique des données officielles DATEX II (Bison Futé) pour la DIRO
- Moissonnage des données via OGC API pour le CD35
- Moissonnage des données via API pour Rennes Metropole
- Moissonnage des données via API pour le CD44
- Moissonnage des données via OGC API pour le CD56

Toutes ces données agrégées seront ensuite visualisées sur une carte interactive via mviewer (Geobretagne).

---

## Objectifs

Le système vise à :
- Centraliser les routes coupées à cause des inondation provenant de sources multiples
- Faciliter la coordination 
- Informer rapidement les usagers de l'état du réseau routier en cas de crise inondation
- Archiver l'historique des événements pour analyse

---

## Fonctionnement général

```
┌─────────────────────┐      ┌──────────────────────┐
│  Agents terrain     │      │   Autres données     │
│  (Saisie Grist)     │      │       d'opérateurs   │
└──────────┬──────────┘      └──────────┬───────────┘
           │                            │
           │Robot : Export API          │ Robot : Moissonnage Python/API/flux WFS
           ▼                            ▼
    ┌──────────────────────────────────────┐
    │     Robot JS ->  Fichiers GeoJSON    │ Cloud Geobretagne
    └──────────────────┬───────────────────┘
                       │
                       ▼
              ┌────────────────┐
              │    mviewer     │
              │  (Visualisation │ Cloud Geobretagne
              │  cartographique)│
              └────────────────┘
```


## Visualisation cartographique

**Accès au visualiseur** : [Carte interactive mviewer](https://geobretagne.fr/app/routes_coupees_inondation)

---

## Licence

Ce projet est distribué sous licence MIT. Voir le fichier [LICENSE](LICENSE) pour plus de détails.

---



