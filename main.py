# from pprint import pprint
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
    Возвращает всех клиентов, если данные из поиска совпадают, например:
    если поиск только по фамилии, то вернутся данные всех однофамильцев

    """
    with conn.cursor() as cur:
        query = """
            SELECT c.id, c.first_name, c.last_name, c.email, p.phone
            FROM clients c
            LEFT JOIN phones p ON c.id = p.client_id
            WHERE
        """
        find = []
        params = []
        if first_name:
            find.append('c.first_name = %s')
            params.append(first_name)
        if last_name:
            find.append('c.last_name = %s')
            params.append(last_name)
        if email:
            find.append('c.email = %s')
            params.append(email)
        if phone:
            find.append('p.phone = %s')
            params.append(phone)

        query += ' AND '.join(find)

        cur.execute(query, params)
        result = cur.fetchall()
        for row in result:
            print(row)

if __name__ == '__main__':
    with psycopg2.connect(database='clients_db', user='postgres', password=password_psq) as conn:
    
        create_db(conn)
        add_client(conn, 'Яков', 'Иванов', 'yak_iv@ya.ru')
        add_client(conn, 'Петя', 'Васечкин', 'vasechkin@ya.ru')
        add_client(conn, 'Василий', 'Теркин', 'vas_ter@internet.ru')
        add_phones(conn, 1, 89089990101)
        add_phones(conn, 1, 83435555555)
        add_phones(conn, 2, '5555')
        add_phones(conn, 3, 88003000600)
        update_client(conn, 2, '', 'Иванов', '')
        delete_phone(conn, 1, '83435555555')
        delete_client(conn,3)
        find_client(conn, '', 'Иванов', '','5555')

conn.close()
