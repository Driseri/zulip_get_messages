import zulip
from pprint import pprint
import psycopg2
import configparser
from prometheus_client import start_http_server, Counter
import time
import logging
import json

logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logging.info('Start script')


config = configparser.ConfigParser()
config.read('config.ini')


# Запуск HTTP-сервера для метрик Prometheus
start_http_server(int(config['prometheus']['port']))

keys = [
    'id', 'sender_id', 'content', 'recipient_id', 'timestamp', 'client', 'subject',
    'topic_links', 'is_me_message', 'submessages', 'flags', 'sender_full_name',
    'sender_email', 'sender_realm_str', 'display_recipient', 'type', 'stream_id',
    'avatar_url', 'content_type'
]


messages_received = Counter('received_messages', 'Количетсво полученных сообщений в итерацию')



# Создание клиента
client = zulip.Client(config_file=config['zulip']['zuliprc'])

table = config['database']['table']

connection = psycopg2.connect(
    dbname=config['database']['dbname'],
    user=config['database']['user'],
    password=config['database']['password'],
    host=config['database']['host'],
    port=config['database']['port']
)

cursor = connection.cursor()

last_id = None

# Запрос на выборку сообщения с самым большим id
query = f"SELECT * FROM {table} ORDER BY id DESC LIMIT 1;"
cursor.execute(query)
message_with_max_id = cursor.fetchone()

pprint(message_with_max_id)
if message_with_max_id:
    last_id = message_with_max_id[0]


while True:
    if last_id:
        response = client.get_messages({
            'anchor': last_id,
            'num_before': 0,
            'num_after': 5,
            'narrow': [],
            'apply_markdown': False,
            'include_anchor': False
        })
        last_id = max(message['id'] for message in response['messages'])
        print(last_id)
        print(response)
        # messages_received.inc(len(response['messages']))


        for mes in response['messages']:
            columns = '''id, sender_id, content, recipient_id, timestamp, client, subject, 
            topic_links, is_me_message, submessages, flags, sender_full_name, 
            sender_email, sender_realm_str, display_recipient, type, stream_id, avatar_url, content_type'''
            placeholders = ", ".join(["%s"] * len(keys))
            values = [mes.get(key) for key in keys]
            query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders});"
            # print(query)
            # print(values)
            try:
                cursor.execute(query, values)
                connection.commit()
            except:
                logging.info('Send Format Error %s', json.dumps(mes, indent=4))
                # print("ОШИБКА"*10)
                # print(query)
                # print('*'*10)
                # print(values)


    else:

        response = client.get_messages({
                'anchor': 'oldest',
                'num_before': 0,
                'num_after': 1,
                'narrow': [],
                'apply_markdown': False,
                'include_anchor': False
            })
        last_id = max(message['id'] for message in response['messages'])
        # print(last_id)
        # print(response)

        # messages_received.inc(len(response['messages']))

        columns = '''id, sender_id, content, recipient_id, timestamp, client, subject, 
        topic_links, is_me_message, submessages, flags, sender_full_name, sender_email, 
        sender_realm_str, display_recipient, type, stream_id, avatar_url, content_type'''

        placeholders = ", ".join(["%s"] * len(keys))

        values = [response['messages'][0].get(key) for key in keys]
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders});"
        try:
            cursor.execute(query, values)
            connection.commit()
        except:
            logging.info('Send Format Error %s', json.dumps(mes, indent=4))

    time.sleep(10)


cursor.close()
connection.close()
