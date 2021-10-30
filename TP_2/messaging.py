## Creation d'un pseudo client de messagerie instantané.
## Exercice : https://github.com/ElMehdiBen/Py_Course/blob/main/Kafka/ExoRT/ExoMessaging.md

# Import Dependencies
import os                       # Fonction liée à l'operating system
from json import dumps, loads
from kafka import KafkaProducer, KafkaAdminClient, KafkaConsumer    # Fonction de gestion Kafka
from dotenv import load_dotenv  # Permet le stockage de données sensible dans un fichier à part

# Path du projet
BASEDIR = os.path.abspath(os.path.dirname(__file__))

# Recuperation des variables d'environnements.
load_dotenv(os.path.join(BASEDIR, '.env'))

# Attribution des constantes
IP_VM = os.getenv("IP_VM")

# Vidage du TOPIC

user_name = input('Enter your name : ')
Admin = KafkaAdminClient(bootstrap_servers = [IP_VM])
Admin.delete_topics([user_name])

# Envoi d'un message

producer = KafkaProducer(bootstrap_servers = [IP_VM],
                         value_serializer = lambda v: dumps(v).encode('utf-8'))

dest = input('Enter your receiver :')
message = input('Enter your message :')
data = {'sender' : user_name, 'receiver' : dest, 'message' : message}
print('{}'.format(data))
producer.send(dest, value = data)

# Listening TOPIC

consumer = KafkaConsumer(
    user_name,
    bootstrap_servers = [IP_VM],
    auto_offset_reset = 'earliest',
    enable_auto_commit = True,
    value_deserializer = lambda x: loads(x.decode('utf-8')))

# Sending a reply if message

for message in consumer:
    message = message.value
    print('{}'.format(message))
    reply = input('Enter your message : ')
    data_rep = {'sender' : user_name, 'message' : reply}
    producer.send(message['sender'], value = data_rep)

