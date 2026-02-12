import sqlite3
from sqlite3 import Error
from faker import Faker

def create_connection(db_file):
   """ create a database connection to a SQLite database """
   conn = None
   try:
       conn = sqlite3.connect(db_file)
       print(f"Connected to {db_file}, sqlite version: {sqlite3.version}")
       return conn
   except Error as e:
       print(e)
       return None


def create_tables(conn):
    table_users = """
        CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        email TEXT
    );
    """

    table_posts = """
        CREATE TABLE IF NOT EXISTS posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        title TEXT NOT NULL,
        content TEXT NOT NULL,
        FOREIGN KEY (user_id) REFERENCES users(id)
    );
    """

    table_comments = """
        CREATE TABLE IF NOT EXISTS comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        post_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        content TEXT NOT NULL,
        FOREIGN KEY (post_id) REFERENCES posts(id),
        FOREIGN KEY (user_id) REFERENCES users(id)
    );
    """
    try:
        c = conn.cursor()
        c.execute(table_users)
        c.execute(table_posts)
        c.execute(table_comments)
    except Error as e:
        print(e)


def execute_sql(conn, sql):
    try:
       c = conn.cursor()
       c.execute(sql)
    except Error as e:
       print(e)


def insert_users(conn, number):
    fake = Faker()
    for _ in range(number):
        username = fake.user_name()
        email = fake.email()
        print(username, email)
        sql = "INSERT INTO users (username, email) values (?, ?)"
        user = (username, email)
        try:
            cur = conn.cursor()
            cur.execute(sql, user)
        except Error as e:
            print(e)
    conn.commit()


def insert_posts(conn):
    fake = Faker()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM users")
        user_ids = [row[0] for row in cur.fetchall()]
        
        for user_id in user_ids:
            title = fake.sentence(nb_words=5)
            content = fake.paragraph(nb_sentences=3)
            cur.execute(
                "INSERT INTO posts (user_id, title, content) VALUES (?, ?, ?)",
                (user_id, title, content)
            )
        conn.commit()
    except Error as e:
        print(e)

def insert_comments(conn):
    fake = Faker()
    try:
        cur = conn.cursor()

        cur.execute("SELECT id FROM posts")
        post_ids = [row[0] for row in cur.fetchall()]

        cur.execute("SELECT id FROM users")
        user_ids = [row[0] for row in cur.fetchall()]
        
        for post_id in post_ids:
            user_id = fake.random_element(elements=user_ids)
            content = fake.paragraph(nb_sentences=3)
            cur.execute(
                "INSERT INTO comments (post_id, user_id, content) VALUES (?, ?, ?)",
                (post_id, user_id, content)
            )
        conn.commit()
    except Error as e:
        print(e)

def select_all_users(conn):
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users")
        rows = cur.fetchall()
        return rows
    except Error as e:
        print(e)
        return []

def print_all_data_from_table(conn, table):
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table}")
        row = cur.fetchone()
        while row:
            print(row)
            row = cur.fetchone()
    except Error as e:
        print(e)


def select_by_id(conn, table, id):
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table} WHERE id = ?", (id,))
        row = cur.fetchone()
        return row
    except Error as e:
        print(e)
        return None


def update(conn, table, row_id, **kwargs):

    columns = ", ".join([f"{k} = ?" for k in kwargs])
    values = tuple(kwargs.values()) + (row_id,)

    sql = f"UPDATE {table} SET {columns} WHERE id = ?"
    
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error: {e}")


def delete(conn, table, id):   
    try:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {table} WHERE id = ?", (id,))
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error: {e}")


if __name__ == '__main__':
   print('main')
   conn = create_connection("database.db")
   print('conn:', conn)
   if conn:
        create_tables(conn)
        insert_users(conn, 5)
        for user_row in select_all_users(conn):
           print(user_row)
        print('*'*10)
        print_all_data_from_table(conn, 'users')
        insert_posts(conn)
        print_all_data_from_table(conn, 'posts')
        insert_comments(conn)
        print_all_data_from_table(conn, 'comments')
        print(select_by_id(conn, 'users', 3))
        update(conn, 'users', 2, username = 'updatedUserName')
        print_all_data_from_table(conn, 'users')
        delete(conn, 'users', 2)
        print_all_data_from_table(conn, 'users')
        conn.close()
