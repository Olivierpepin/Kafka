## Récupération des données d'une API ayant transité par Kafka pour envoi à la DB de Elestic Search
## Exercice : https://github.com/ElMehdiBen/Py_Course/blob/main/Kafka/ExoRT/Exo.md

# Import dependencies
import os                       # Fonction liée à l'operating system
from kafka import KafkaConsumer             # Permet la creation d'un consumer
from json import loads
from elasticsearch import Elasticsearch     # Permet l'envoi à ES
from datetime import datetime
from dotenv import load_dotenv  # Permet le stockage de données sensible dans un fichier à part

# Path du projet
BASEDIR = os.path.abspath(os.path.dirname(__file__))

# Recuperation des variables d'environnements.
load_dotenv(os.path.join(BASEDIR, '.env'))

# Attribution des constantes
IP_VM = os.getenv("IP_VM")
ES = os.getenv("ES")

# Creation d'un consumer
consumer = KafkaConsumer(
    'eth_price',
     bootstrap_servers = [IP_VM],
     auto_offset_reset = 'earliest',
     enable_auto_commit = True,
     value_deserializer = lambda x: loads(x.decode('utf-8')))

# Utilisation d'une boucle (earliest pour envoyer tous les messages du topic et s'arrête, latest seulement les derniers)
for message in consumer:
    message = message.value
    doc = {                         # Creation d'un format de données
    'type' : 'ETH',
    'price': message["k_price"],
    'timestamp': datetime.now()}
    print('{}'.format(message))
    es = Elasticsearch([ES])   # variable ES
    res = es.index(index="eth_price", document=doc)     # Envoi en index sur ES
    print(res)

# Le résultat est visible directement sur ES (extension Elasticvue par exemple)



