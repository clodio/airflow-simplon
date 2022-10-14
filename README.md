# Intro Airflow

## Contexte du projet

Vous devez notamment évaluer sa capacité à :

* lancer des applications en ligne de commande
* lancer des applications écrites en python
* effectuer des tâches en parallèle
* attendre que plusieurs tâches en parallèle soient terminées avant d’en lancer une nouvelle

​

### optionnel

* générer un fichier dont le nom contient la date d'exécution du workflow
* rejouer de manière idempotente une exécution passé
* transmettre un identifiant entre les différentes tâches
* utiliser un sensor pour détecter l'arrivé d'un fichier
* créer dynamiquement des tâches dans un DAG à partir d'une list de string python
* créer et utiliser des variables airflow
* créer et utiliser des connections airflow pour se connecter à une base de donnée (postgresql)
* créer son propre opérateur qui execute des requêtes hive

## Modalités pédagogiques

lancer airflow

**airflow standalone**


## Actions

dans le dossier "Dag" de airflow situé dans le dossier de l'utilisateur simplon créer un DAG vide :

* se lançant toutes les heures
* démarrant ce matin à 9H

vérifier dans l'UI airflow que le DAG est bien présent (http://localhost:8080)

Ajouter un dummy operator au DAG et le nommer start vérifier dans l'UI airflow que le dummy operator est apparu

Ajouter une tâche utilisant le bash operator la nommer "list" et lui faire exécuter la commande "ls"

Utiliser l'opérateur bitshift (>>) pour faire en sorte que la tâche "list" soit exécutée après la tâche "start"

Ajouter une tâche utilisant le python opérateur la nommer "hello" et lui faire écrire un fichier contenant le mot "world" et contenant la date d'execution du workflow.

Créer deux tâches utilisant le bash opérator: la tâche nommée "sleep10" devra faire un sleep de 10 secondes la tache nommée "sleep15" devra faire un sleep de 15 secondes.

faire s'exécuter ces deux tâches en parallèle après la tâche "hello"

Créer une tâche nommée "join" qui utilise le dummy operator et qui attend que les tâches sleep10 et sleep15 soient terminées pour s'exécuter.

Votre workflow est maintenant complet vous pouvez l'activer et regarder ce qu'il se passe.

## Livrables

Un DAG airflow démontrant les capabilités de l'outil

voir airflow.py