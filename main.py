from pprint import pprint
import psycopg2
from psycopg2.sql import Identifier, SQL
from dotenv import load_dotenv
import os.path

dotenv_path = 'config_example.env'
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
password_psq = os.getenv('password_psq')

def create_db(conn):
    """
    создание таблиц
    :param conn:

    """
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE if exists clients_phones;
        DROP TABLE if exists phones;
        DROP TABLE if exists clients;
        """)
        cur.execute("""
            CREATE TABLE if not exists clients(
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(40) NOT NULL,
            last_name VARCHAR(40) NOT NULL,
            email VARCHAR(100) UNIQUE);
        """)
        cur.execute("""
                CREATE TABLE if not exists phones(                
                client_id integer references clients(id),
                phone text UNIQUE);
        """)

def add_client(conn, first_name, last_name, email):
    """
    наполнение таблиц (C из CRUD)
    :param conn:
    :param first_name:
    :param last_name:
    :param email:
    :return:
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO clients(first_name, last_name, email) 
            VALUES(%s, %s, %s)
            RETURNING id, first_name, last_name, email;
            """, (first_name, last_name, email))
        return cur.fetchall()

def add_phones(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
                INSERT INTO phones(client_id, phone) 
                VALUES(%s, %s)
                RETURNING client_id, phone;
                """, (client_id, phone))
        return cur.fetchall()

def update_client(conn, id=None, first_name=None, last_name=None, email=None):
    """"
    Обновление данных клиента

    """
    with conn.cursor() as cur:
        clients_list = {'first_name': first_name, 'last_name': last_name, 'email': email}
        for key, item in clients_list.items():
            if item:
                cur.execute(SQL(
                        'UPDATE clients SET {}=%s WHERE id = %s').format(Identifier(key)),
                        (item, id))

def delete_phone(conn, client_id, phone):
    """"
    Удаление телефонного номера клиента
    """
    with conn.cursor() as cur:

        cur.execute("""
                DELETE FROM phones WHERE client_id = %s AND phone = %s
                RETURNING client_id;
                """, (client_id, phone))
        return cur.fetchall()

def delete_client(conn, id=None):
    """"
    Удаление клиента,
    а также телефонного номера
    """
    with conn.cursor() as cur:
        cur.execute("""
                DELETE FROM phones WHERE client_id = %s;                                                
                """, (id, ))
    with conn.cursor() as cur:
        cur.execute("""
                DELETE FROM clients WHERE id = %s;                                                
                """, (id, ))

def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    """"
    Поиск клиента по его данным.
    Возвращает информацию о клиентах в виде:
    id, first_name, last_name, email, phone

    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.id, c.first_name, c.last_name, c.email, p.phone
            FROM clients c
            LEFT JOIN phones p ON c.id = p.client_id
            WHERE (first_name = %(first_name)s OR %(first_name)s IS NULL)
            OR (last_name = %(last_name)s AND %(last_name)s IS NULL)
            OR (email = %(email)s OR %(email)s IS NULL)            
            OR (phone = %(phone)s OR %(phone)s IS NULL);
        """, {'first_name': first_name, 'last_name': last_name, 'email': email, 'phone': phone})
        return cur.fetchall()

if __name__ == '__main__':
    with psycopg2.connect(database='clients_db', user='postgres', password=password_psq) as conn:
    
        create_db(conn)
        add_client(conn, first_name='Яков', last_name='Иванов', email='yak_iv@ya.ru')
        add_client(conn, first_name='Петя', last_name='Васечкин', email='vasechkin@ya.ru')
        add_client(conn, first_name='Василий', last_name='Теркин', email='vas_ter@internet.ru')
        add_phones(conn, client_id=1, phone=89089990101)
        add_phones(conn, client_id=1, phone=83435555555)
        add_phones(conn, client_id=2, phone='5555')
        add_phones(conn, client_id=3, phone=88003000600)
        update_client(conn, id=2, first_name='', last_name='Иванов', email='')
        delete_phone(conn, client_id=1, phone='83435555555')
        delete_client(conn,id=3)
        pprint(find_client(conn, first_name='Яков', last_name='Иванов', email='', phone=''))

conn.close()
