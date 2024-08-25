from kafka import KafkaConsumer
from json import loads
import clickhouse_connect

def create_table_if_not_exist(db):
    query = '''create table if not exists db.user_trackers (
        id UUID default generateUUIDv4(),
        user_id Int32,
        name varchar(32),
        message TEXT,
        created_at timestamp default now()
    ) engine = MergeTree
    primary key(id);
    '''
    db.command(query)


consumer = KafkaConsumer(
    'user_tracker',
    bootstrap_servers='localhost:9092',
)

clickhouse = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    database='db',
    username='clickhouse',
    password='clickhouse_password'
)

create_table_if_not_exist(db=clickhouse)
print("worker started")

for message in consumer:
    message_data = loads(str(message.value.decode('utf-8')))
    print(message_data)
    clickhouse.command("insert into db.user_trackers (user_id, name, message) values ({}, '{}', '{}');".format(message_data['user_id'], message_data['name'], message_data['message']))
    # print(f"Received message: {message.value}")