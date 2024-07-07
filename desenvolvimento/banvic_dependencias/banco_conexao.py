import psycopg2 as pg
from dotenv import load_dotenv 
import os

load_dotenv()

def conexao():
    conexao = pg.connect(database=os.getenv("database"),
                    user=os.getenv("user"),
                    password=os.getenv("password"),
                    host=os.getenv("host"),
                    port=os.getenv("port")
                     )
    conectado = conexao.cursor()

    conexao.autocommit = True
    cursor = conexao.cursor()
    return cursor