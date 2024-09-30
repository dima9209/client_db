import psycopg2 as ps


def client_existing(cur, client_id):
    cur.execute("""
            SELECT id FROM clients WHERE id = %s;""", (client_id,))
    return cur.fetchone()


def create_db(conn):
    """Function for creating table structures in the database. If the tables were created previously, the table structure will be recreated."""
    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS phone;')
    cur.execute('DROP TABLE IF EXISTS clients;')
    cur.execute('''CREATE TABLE IF NOT EXISTS clients(
    id serial PRIMARY KEY,
    first_name varchar(50) NOT NULL,
    last_name varchar(50) NOT NULL,
    email varchar(50) NOT NULL);
                ''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS phone(
    id serial PRIMARY KEY,
    phone_number varchar(11) not null,
    client_id integer references clients(id) not null );''')
    conn.commit()
    cur.close()
    return 'Структуры таблиц созданы успешно!'


def add_client(conn, first_name, last_name, email, phones=None):
    """Function for creating a record for a new client."""
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO clients(first_name, last_name, email)
            VALUES(%s, %s, %s) returning id;''', (first_name, last_name, email))
        if phones is not None:
            client_id = cur.fetchone()[0]
            cur.execute('''
            INSERT INTO phone(phone_number, client_id)
            VALUES(%s, %s);''', (str(phones), client_id))
        conn.commit()
    return 'Новый клиент добавлен!'


def add_phone(conn, client_id, phone):
    """Function for adding a phone number for an existing client."""
    with conn.cursor() as cur:
        is_client = client_existing(cur, client_id)
        if is_client is None:
            return 'Клиент с таким id не существует'
        else:
            cur.execute("""
             INSERT INTO phone(phone_number, client_id)
             VALUES(%s, %s);""", (str(phone), client_id))
            conn.commit()
            return 'Номер телефонм клиента успешно добавлен.'


def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    """The function changes the data about the client"""
    counter = 0
    with conn.cursor() as cur:
        if client_existing(cur, client_id) is None:
            return 'Клиент с таким id не существует'

        for key, value in zip(['first_name', 'last_name', 'email', 'phones'], (first_name, last_name, email, phones)):
            if value is not None:
                if key in ['first_name', 'last_name', 'email']:
                    cur.execute(f"""
                    UPDATE clients SET
                    {key} = %s
                    WHERE id = %s;""", (value, client_id))

                else:
                    cur.execute("""
            SELECT id FROM clients WHERE id = %s;""", (client_id,))
                    is_phone = cur.fetchone()
                    if is_phone:
                        cur.execute(f"""UPDATE phone SET
                    phone_number = %s
                    WHERE client_id = %s;""", (str(value), client_id))
                        conn.commit()
                    else:
                        add_phone(conn, client_id, value)
                counter += 1
    if counter:
        return 'Данные успешно изменены'
    else:
        return 'Не введены параметры обновления'


def delete_phone(conn, client_id, phone):
    """The function of deleting phone numbers"""
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phone WHERE client_id = %s AND phone_number = %s;""", (client_id, str(phone)))
        conn.commit()
        return 'Телефон успешно удален'


def delete_client(conn, client_id):
    """Function for deleting clients"""
    with conn.cursor() as cur:
        cur.execute('DELETE FROM phone WHERE client_id = %s;', (client_id,))
        cur.execute('DELETE FROM clients WHERE id = %s;', (client_id,))
        conn.commit()

        return 'Клиент удален'


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    """Customer information search function"""
    params_find = {key + '= %s':value for key, value in zip(['first_name', 'last_name', 'email', 'phone'], [first_name, last_name, email, phone])if value is not None}
    with conn.cursor() as cur:
        cur.execute(f"""SELECT first_name, 
                                last_name, 
                                email, 
                                phone_number
                       FROM clients
                       join phone ON clients.id = phone.client_id
                       WHERE {' AND '.join(params_find.keys())};
                       """, tuple(params_find.values()))
        clients = cur.fetchall()
        for client in clients:
            print('Информация о клиенте')
            print('Имя: ', client[0])
            print('Фамилия: ', client[1])
            print('email:', client[2])
            print('Телефон:', client[3])


conn = ps.connect(database='clientdb', user='postgres', password='......')
# Создание структур таблиц БД

print(create_db(conn))
print()

# Добавление нового клиента
print(add_client(conn, 'Иван', 'Иванов', 'ivanov@mail.ru', 89991111111))
print(add_client(conn, 'Петр', 'Петров', 'petr@mail.ru'))
print(add_client(conn, 'Дмитрий', 'Сидоров', 'sidr@mail.ru', 89999999999))
print(add_client(conn, 'Мария', 'Кондратюк', 'mario@mail.ru', 85555555555))
print()
# Добавление номера телефона к существующему клиенту.
print(add_phone(conn, 10, 89144343644))
print(add_phone(conn, 2, 89224851732))
print(add_phone(conn, 3, 84444444444))
print()
# Изменение данных клиента
print(change_client(conn, 2, 'Сергей', phones=81111111111))
print(change_client(conn, 1))
print()
#Удаление телефона
print(delete_phone(conn, 3, 89999999999))
print()
#Удаление клиента
print(delete_client(conn, 1))
print()
# Поиск клиента
find_client(conn, first_name='Дмитрий')
