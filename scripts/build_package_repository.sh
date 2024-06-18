#!/bin/sh

# Vérifier si le nombre d'arguments est correct
if [ $# -ne 2 ]; then
  echo "Usage: $0 project_name package_name"
  exit 1
fi

# Assigner les arguments à des variables
project_name=$1
package_name=$2

echo "Building Package Repository for $project_name known as $package_name package"

#Copier le fichier .gitignore dans template_files
echo "Copie du .gitignore"
cp /mnt/c/users/mangg/template_files/.gitignore .gitignore
echo "Le fichier.gitignore a été copié avec succès "
echo ""

#Créer un repository git:
echo "Initialisation du repository git"
git init
echo "Le repository git a été initialisé avec succès"
echo ""

#Ajouter les fichiers de la structure de base et créer un commit d'initialisation
echo "Ajout des fichiers de la structure de base et création un commit d'initialisation"
git add -A
git commit -m "Initial commit for $project_name known as $package_name package"
echo "Le commit initial a été créé avec succès"
echo ""

#Créer un repository Github avec gh
echo "Création d'un nouveau repo GitHub"
gh repo create --source=. --private --push
gh browse --repo $project_name
echo "Le repository GitHub a été créé avec succès"
echo ""

#Créer un tag initial
echo "Création du tag initial"
git tag -a v0.0.0 -m "Initial version of the package"
echo "Le tag initial a été créé avec succès"
echo ""

#Ajouter le remote origin
echo "Ajout du remote origin"
git remote add origin
echo "Le remote origin a été ajouté avec succès"
echo ""

#Push du commit initial sur GitHub
echo "Push du commit initial"
git push origin master
echo "Le commit initial a été pushé avec succès"
echo ""
