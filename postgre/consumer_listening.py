## Creation d'un consumer écoutant un topic Kafka avec filtre sur une donnée du topic

# Import dependancies

import os                           # Fonction liée à l'operating system (Windows dans ce cas)
from kafka import KafkaConsumer     # Permet l'ecoute de topics Kafka
from json import loads
from dotenv import load_dotenv      # Permet le stockage de données sensible dans un fichier à part


# Path du projet
BASEDIR = os.path.abspath(os.path.dirname(__file__))

# Recuperation des variables d'environnements.
load_dotenv(os.path.join(BASEDIR, '.env'))

# Attribution des variables
IP_VM = os.getenv("IP_VM")


# Creation du consumer
consumer = KafkaConsumer(
    'messages',                                                # Nom du topic ecouté
     bootstrap_servers = [IP_VM],                               # adresse ip + port du cluster Kafka
     auto_offset_reset = 'earliest',                            # type de recuperation des messages du topic (earlist pour tous ou lastest pour le dernier)
     enable_auto_commit = True,
     value_deserializer = lambda x: loads(x.decode('utf-8')))

# Boucle d'écoute sur le terme Olivier (latest pour ecoute, earliest recuperera le fil et s'arretera)

for message in consumer:
    message = message.value
    print('{}'.format(message))

#if message['receiver'] == "Olivier":