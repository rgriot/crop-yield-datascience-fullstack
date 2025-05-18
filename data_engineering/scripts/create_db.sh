#!/bin/bash

# === CONFIGURATION ===
UTILISATEUR="postgres"
MOT_DE_PASSE="thohn7No!"
BDD="crop_yield_db"
HOTE="localhost"

# === 2. Créer la base de données vide si elle n'existe pas ===
echo "🔍 Vérification de l'existence de la base '$BDD'..."
createdb -U $UTILISATEUR -h $HOTE $BDD