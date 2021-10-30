## Creation d'un producer qui query une DB Postgre, tranforme les données afin de les envoyer à Kafka.

# Import dependencies

import psycopg2                 # Connecteur Postgre
import os                       # Fonction liée à l'operating system
from kafka import KafkaProducer # Permet la creation d'un producer
from json import dumps
from dotenv import load_dotenv  # Permet le stockage de données sensible dans un fichier à part
from time import sleep          # Fonction wait

# Path du projet
BASEDIR = os.path.abspath(os.path.dirname(__file__))

# Recuperation des variables d'environnements.
load_dotenv(os.path.join(BASEDIR, '.env'))

# Attribution des constantes
IP_VM = os.getenv("IP_VM")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DATABASE = os.getenv("DATABASE")

# Connection DB Postgre
connection = psycopg2.connect(user=USER,
                                password=PASSWORD,
                                host=HOST,
                                port=PORT,
                                database=DATABASE)

# Attribution d'une connection au curseur
cursor = connection.cursor()

# Query Postgre
cursor.execute("select * from public.customers")

# Creation du producer et gestion de l'encodage
producer = KafkaProducer(bootstrap_servers = [IP_VM],
                         value_serializer = lambda v: dumps(v).encode('utf-8'))

# Recupere la premiere ligne du curseur et execute une boucle tant que les lignes ne sont pas vides
row = cursor.fetchone()
while row is not None:
    data = {'ID': row[0], 'DOB': row[1], 'Gender': row[2], 'City': row[3]}  # Creation d'un format de données
    print('{}'.format(data))
    producer.send('customers', value = data)                                # Envoi au topic customers de Kafka
    row = cursor.fetchone()                                                 # Recuperation d'une nouvelle ligne
    sleep(1)                                                                # Fonction d'attente