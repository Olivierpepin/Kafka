## Creation d'un producer avec interrogation d'une API en Real Time avec envoi à un topic Kafka
## REAL TIME API EXAMPLE  Source : https://rapidapi.com/Coinranking/api/coinranking1/
## Exemple : https://github.com/ElMehdiBen/Py_Course/blob/main/Kafka/ExoRT/Exo.md

# Import dependencies
import os                       # Fonction liée à l'operating system
import requests
from json import dumps
from kafka import KafkaProducer # Permet la création d'un producer
from time import sleep
from dotenv import load_dotenv  # Permet le stockage de données sensible dans un fichier à part

# Path du projet
BASEDIR = os.path.abspath(os.path.dirname(__file__))

# Recuperation des variables d'environnements.
load_dotenv(os.path.join(BASEDIR, '.env'))

# Attribution des constantes
IP_VM = os.getenv("IP_VM")
KEY_API = os.getenv("KEY_API")

# Creation du producer
producer = KafkaProducer(bootstrap_servers = [IP_VM],
                         value_serializer = lambda v: dumps(v).encode('utf-8'))

# Boucle de query de l'API avec envoi sur Kafka après un transform du JSON
# Recuperation du snippet directement sur Fast API
# Meilleur vue du JSON possible sur un formateur : https://jsonformatter.curiousconcept.com/#
for e in range(10):
    url = "https://coinranking1.p.rapidapi.com/coin/2"
    headers = {
    'x-rapidapi-host': "coinranking1.p.rapidapi.com",
    'x-rapidapi-key': KEY_API
    }
    response = requests.request("GET", url, headers=headers).json() # Recuperation JSON
    price = response['data']['coin']['price']                       # Crawling pour récupération des données
    temps = response['data']['coin']['listedAt']
    data = {'k_price' : price, 'k_temps': temps}                    # Creation du format d'envoi à Kafka
    print('{}'.format(data))
    producer.send('eth_price', value = data)                        # Envoi sur un topic Kafka
    sleep(60)                                                       # Attente de la boucle (MAJ de l'API toute les minutes)

# Récupérer des chiffres en string n'est pas pertinent.
# Il serai tout à fait possible de changer le format avant de l'envoyer à Kafka avec par exemple :
#    price_float = float(price)
#    price_float = "{:.2f}".format(price_float)


