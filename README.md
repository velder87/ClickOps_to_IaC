# 🚀 De "Click-Ops" à DevOps : Votre Infrastructure Data Azure en 3 minutes

Bienvenue dans le dépôt de code accompagnant l'article de **L'Atelier BI**.

Ce projet démontre comment abandonner la création manuelle de ressources via le portail Azure ("Click-Ops") pour adopter une approche **Infrastructure as Code (IaC)** robuste, reproductible et sécurisée.

![Azure Architecture Diagram](link_to_your_diagram.png)
*(Note : Remplacez ce lien par l'image de votre architecture générée précédemment)*

## 🎯 Objectif

Déployer une "Modern Data Stack" complète sur Azure en une seule commande, comprenant :
* **Resource Group** : Conteneur logique.
* **Azure Data Lake Gen2** : Stockage hiérarchique pour vos données brutes et traitées.
* **Azure Data Factory (ADF)** : Orchestration des pipelines ETL/ELT.
* **Azure Key Vault** : Gestion sécurisée des secrets.
* *(Optionnel)* **Azure Databricks** : Environnement de calcul distribué.

## 🛠️ Prérequis

Avant de lancer le déploiement, assurez-vous d'avoir :
1.  Un compte **Azure** actif (une souscription active).
2.  **Azure CLI** installé et configuré (`az login`).
3.  **Terraform** installé (v1.0+).

## 📂 Structure du Projet

```text
.
├── main.tf           # Définition des ressources principales
├── variables.tf      # Déclaration des variables (noms, régions, tiers)
├── outputs.tf        # Informations retournées après déploiement (URLs, IDs)
├── provider.tf       # Configuration du provider Azure
└── README.md         # Ce fichier
