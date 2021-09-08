import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """ Conexão ao Postgres, para criar uma base de dados
    chamada sparkify, caso ela já exista, será excluída e recriada.
    """
    # connect to default database.
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    # Nesta primeira conexão é necessário ser efetuada com qualquer bando de dados que já exista,
    # podendo ser no próprio banco de dados padrão "postgres". Porta padrão 5432.
    # O usuário também deve ser criado anteriormente:
        # Criação de usuário no postgres:
        # CREATE USER usuario SUPERUSER INHERIT CREATEDB CREATEROLE;
        # ALTER USER usuario PASSWORD 'senha';

    conn.set_session(autocommit=True)
    # O autocommit é desligado por padrão no psycopg2, ele é ativado geralmente para diminuir a probabilidade
    # de acidentalmente uma transação ser esquecida e permanecer ativa, causando travamentos e trazendo problemas ao autovacuum.
    
    cur = conn.cursor() 
    # cur será a estrutura de controle para percorrer os registros utilizando o cursor().

    # create sparkify database with UTF8 encoding.
    cur.execute("DROP DATABASE IF EXISTS sparkifydb") # Deletar caso exista sparkifydb.
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0") # Criar o banco de dados sparkifydb com utf8.

    # close connection to default database.
    conn.close()

    # connect to sparkify database.
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    """ Função para chamar o sql_queries.py, para dropar uma tabela com esquema estelar.
    caso ele já exista. 
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ Função para chamar o sql_queries.py para criar as tabelas com esquema estelar.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Esse programa executará as funções para criar as bases de dados e tabelas.
    """
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
