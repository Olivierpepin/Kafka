## Creation d'un consumer écoutant un topic Kafka puis envoi les données du topic suivant un template 
## pour integration Postgre

# Import dependencies

import psycopg2                 # Connecteur Postgre
import os                       # Fonction liée à l'operating system
from kafka import KafkaConsumer # Permet l'ecoute de topics Kafka
from json import loads
from dotenv import load_dotenv  # Permet le stockage de données sensible dans un fichier à part

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

# Creation du consumer
consumer = KafkaConsumer(
    'customers',
     bootstrap_servers = [IP_VM],
     auto_offset_reset = 'earliest',
     enable_auto_commit = True,
     value_deserializer = lambda x: loads(x.decode('utf-8')))

# Connection à la DB Postgre
connection = psycopg2.connect(user=USER,
                                password=PASSWORD,
                                host=HOST,
                                port=PORT,
                                database=DATABASE)

# Creation du template pour insertion dans une table Postgre
INSERT = """INSERT INTO public.customers(
    customer_id, dob, gender, city_code)
    VALUES (%s, %s, %s, %s);"""

# Boucle ecoute du topic et envoi des données sur une table Postgre
for message in consumer:
    cursor = connection.cursor()        # le cursor descendra un à un les rows du topic
    message = message.value             # Récuperation des données du message via le .value
    cursor.execute(INSERT, (message['ID'], message['DOB'], message['Gender'], message['City'])) # Format d'envoi
    connection.commit()                 # Envoi à Postgre
    print('{}'.format(message))