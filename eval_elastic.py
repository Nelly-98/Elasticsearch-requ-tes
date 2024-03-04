#! /usr/bin/python
from elasticsearch import Elasticsearch, helpers
import json
import warnings
import csv

import warnings
warnings.filterwarnings("ignore")

# Connexion au cluster
client = Elasticsearch(hosts = "http://@localhost:9200")

with open('Womens_Clothing.csv', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    helpers.bulk(client, reader, index='eval')

# Récupération du mapping
template = client.indices.get_mapping(index="eval")

# Sauvegarde dans un fichier JSON
with open("{}.json".format("index_template"), "w") as f:
  json.dump(dict(template), f, indent=2)

# Dictionnaire pour stocker les requêtes et leur numéro
requetes = {
    # Une recherche "match_all" de votre nouvel index nommé impérativement "eval"
    "1-2": {"query": {"match_all": {}}},

    #Établir les valeurs uniques:
    #Établir le nombre de valeurs uniques pour le champ "Division Name"
    "2-1": {"size":0, "aggs": {"unique_makes": {"cardinality": {"field": "Division Name.keyword"}}}},

    # Établir le nombre de valeurs uniques pour le champ "Department Name"
    "2-2": {"size":0, "aggs": {"unique_makes": {"cardinality": {"field": "Department Name.keyword"}}}},

    #Établir le nombre de valeurs uniques pour le champ "Class Name"
    "2-3": {"size":0, "aggs": {"unique_makes": {"cardinality": {"field": "Class Name.keyword"}}}},

    #Combien d'articles sont disponibles dans le dataset ?
    "2-4": {"query": {"match_all": {}}},

    #Etablir les appartenances
    # Déterminer le nombre d'articles du champ "Department Name" appartenant à sa Division (champ "Division Name").
    "2-5": {"size":0, "aggs": {"by_division": {"terms":{"field": "Division Name.keyword"}, "aggs": {"by_department": {"terms": {"field": "Department Name.keyword"}}}}}},

    #Déterminer les articles uniques du champ "Department Name".
    "2-6": {"size":0, "aggs": {"unique_department": {"terms": {"field": "Department Name.keyword"}}}},

    #Vérifier l’existence ou non de valeurs nulles dans le jeu de données
    "3": {"size":0, "aggs": {"missing_fields": {"missing": {"field": "Department Name.keyword"}}}},
    #Établir les premières statistiques du jeu de données.
    #Créer un histogramme du champ "age" avec une valeur d'intervalle à "20"
    "4-1": {"size":0, "aggs": {"age_histogram": {"histogram": {"field": "age", "interval": 20}}}},

    #Faire une analyse statistique des notes, déterminer la moyenne et la médiane parmi tous les produits présents dans le dataset
    "4-2": {"size":0, "aggs": {"notes_stats": {"stats": {"field": "rating"}}}},

    #Faire une agrégation des notes pour chaque classe de produit (Champ "Class Name")
    "4-3": {"size":0, "aggs": {"class_rating": {"terms": {"field": "Class Name.keyword"}, "aggs": {"average_rating": {"avg": {"field": "rating"}}}}}},

    #Avec votre histogramme "age", créer un bucket des produits "Class Name" et déterminer les articles les plus représentés selon l'âge des clients.
    "4-4": {"size":0, "aggs": {"age_histogram": {"histogram": {"field": "age", "interval": 20}, "aggs": {"by_class_name": {"terms": {"field": "Class Name.keyword"}}}}}},

    #Analyses avancées
    #Quels sont les termes les plus présents parmi les articles les MIEUX notés ?
    "5-1": {"size": 0,"query": {"range": {"Rating": {"gte": 4}}},"aggs": {"popular_terms": {"terms": {"field": "Review Text.keyword"}}}},

    #Quels sont les termes les plus présents parmi les articles les MOINS bien notés ?
    "5-2": { "size": 0,"query": {"range": {"Rating": {"lt": 3}}},"aggs": {"popular_terms": {"terms": {"field": "Review Text.keyword"}}}},

    #Quels produits votre client devrait garder en priorité dans son catalogue ?
    "5-3": {"size": 0,"aggs": {"top_rated": {"terms": {"field": "Product ID.keyword", "order": {"average_rating": "desc"}},"aggs": {"average_rating": {"avg": {"field": "rating"}}}}}},

    #À l'inverse, sur quels produits votre client NE devrait PAS investir ?
    "5-4": {"size": 0,"aggs": {"low_rated": {"terms": {"field": "Product ID.keyword", "order": {"average_rating": "asc"}},"aggs": {"average_rating": {"avg": {"field": "rating"}}}}}}
}

# Exécute et sauvegarde chaque requête et sa réponse
for question_number, query in requetes.items():
    response = client.search(index="eval", body=query)
    
    # Sauvegarde de la requête et la réponse dans des fichiers JSON
    with open(f"responses/r_{question_number}_response.json", "w") as f:
        json.dump(dict(response), f, indent=2)
    
    with open(f"requests/q_{question_number}_request.json", "w") as f:
        json.dump(query, f, indent=2)
