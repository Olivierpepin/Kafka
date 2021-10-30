## Creation d'un producer permettant l'envoi de message sur un topic Kafka

# Import dependencies

import os                       # Fonction liée à l'operating system
from json import dumps
from random import choice       # Permet une fonction pseudo-aléatoire
from kafka import KafkaProducer # Permet la creation d'un producer
from dotenv import load_dotenv  # Permet le stockage de données sensible dans un fichier à part

# Path du projet
BASEDIR = os.path.abspath(os.path.dirname(__file__))

# Recuperation des variables d'environnements.
load_dotenv(os.path.join(BASEDIR, '.env'))

# Attribution des constantes
IP_VM = os.getenv("IP_VM")

# Creation du producer
producer = KafkaProducer(bootstrap_servers = [IP_VM],
                         value_serializer = lambda v: dumps(v).encode('utf-8'))

# Liste des differents interlocuteurs
receivers = ["Olivier", "Charlene", "Laurent", "Noura", "Malick", "Anne-Cha", "Pascal", "Zeyno", "Ludwig", "Tristan", "Camille"]

message = input('Enter your message:')
data = {'sender': "Olivier", 'receiver': choice(receivers), 'message' : message}    # Format envoi de données avec une fonction random
print('{}'.format(data))
producer.send('messages', value = data)                                             # Envoi des données à Kafka