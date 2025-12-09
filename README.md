# Système de signalement et de visualisation des routes coupées en cas d'inondation

Plateforme de collecte GRIST et de visualisation cartographique mviewer sur Geobretagne des perturbations routières en cas d'inondation en Bretagne (périmètre élargi)

---

## Présentation

Système de collecte GRIST, de moissonnage (API/flux) et de visualisation des routes coupées (mviewer) en cas d'inondations sur le réseau routier.

Le projet combine plusieurs sources de données complémentaires :
- Moissonnage des données saisies dans Grist par les opérateurs (Redon agglomération)
- Moissonnage automatique des données officielles DATEX II (Bison Futé) pour la DIRO
- Moissonnage des données via API pour le CD35
- Moissonnage des données via API pour Rennes Metropole
- Moissonnage des données via ? pour le CD44
- Moissonnage des données via ? pour le CD56

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

---

## Sources de données

### Base Grist

**Utilisation** : Saisie collaborative des signalements terrain par les agents de la DIR Ouest

**Accès** : [Saisie GRIST des routes coupées inondation](https://grist.dataregion.fr/o/inforoute/n9MXV7uiYFKH/Route-coupees-35/p/2)

**Export** : Automatique grâce à l'API vers format GeoJSON

**Contenu** : Signalements manuels (ponctuel ou linéaire) incluant la localisation, le type d'obstacle, les dates de début et de fin, et une description détaillée

### Flux DATEX II (Bison Futé)

**Utilisation** : Récupération automatique des événements routiers officiels

**Norme** : DATEX II version 2.0 (standard européen d'échange de données routières)

**Filtres appliqués** :
- Zone géographique : DIR Ouest uniquement
- Type d'événement : EnvironmentalObstruction (obstructions environnementales)
- Sous-type : flooding (inondation) et flashFloods (crue éclair)

**Fréquence de mise à jour** : Temps réel (rafraîchissement environ toutes les 6 minutes)

### API CD35 

**Utilisation** : Récupération automatique des événements routiers officiels

**Norme** : 

**Filtres appliqués** :


**Fréquence de mise à jour** : 

---

## Visualisation cartographique

**Accès au visualiseur** : [Carte interactive mviewer](https://geobretagne.fr/mviewer/?config=/apps/grist2mviewer/config.xml)

---

## Licence

Ce projet est distribué sous licence MIT. Voir le fichier [LICENSE](LICENSE) pour plus de détails.

---

**DDTM35 - METSSI**

Courriel technique : 

---

## Références

- [DATEX II - Site officiel du standard européen](https://datex2.eu/)
- [Bison Futé - Données ouvertes](https://www.bison-fute.gouv.fr/)

  

---

