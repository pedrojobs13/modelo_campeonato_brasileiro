import pandas as pd
import psycopg2 as postgres
import os
from dotenv import load_dotenv

def connectDatabase():
    try: 
        conn = postgres.connect(database="brasileirao",
                        host="localhost",
                        user=os.getenv("USER"),
                        password=os.getenv("PASSWORD"),
                        port="5432")
        return conn.cursor()
    except postgres.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
    
def createCartaoDatabaseIfNotExist(cursor):
        
    pathCartoes = 'campeonato-brasileiro-cartoes.csv'
    colNames = ["partida_id", "rodata", "clube", "cartao", "atleta", "num_camisa", "posicao", "minuto"]
    cartoesCsv= pd.read_csv(pathCartoes, names=colNames)
    
    nameColumnsDatabase = cartoesCsv.head()
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            {colNames[0]} BIGINT NOT NULL,
            {colNames[1]} BIGINT NOT NULL,
            {colNames[2]} VARCHAR(100),
            {colNames[3]} VARCHAR(100),
            {colNames[4]} VARCHAR(100),
            {colNames[5]} VARCHAR(100),
            {colNames[6]} VARCHAR(100),
            {colNames[7]} VARCHAR(100) 
        )    
    """
    print(create_table_sql)
    
    cursor.execute(create_table_sql)

def main():
    load_dotenv()
    cursor = connectDatabase()
    createCartaoDatabaseIfNotExist(cursor)

    cursor.close()
main()