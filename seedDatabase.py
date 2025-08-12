import pandas as pd
import psycopg2 as postgres
import os
from dotenv import load_dotenv
import numpy as np

# Planilha de cartões 
def connectDatabase():
    try: 
        conn = postgres.connect(database="brasileirao",
                        host=os.getenv("HOST"),
                        user=os.getenv("DATABASEUSER"),
                        password=os.getenv("PASSWORD"),
                        port=os.getenv("PORT"))
        return conn
    except postgres.Error as e:
        print(f"Error connecting to PostgreSQL: {e}") 

def returnCartoesCsv():
    pathCartoes = 'planilhas/campeonato-brasileiro-cartoes.csv'
    colNames = ("partida_id", "rodata", "clube", "cartao", "atleta", "num_camisa", "posicao", "minuto")
    table_name = "cartoes"
    cartoesCsv= pd.read_csv(pathCartoes, names=colNames,header=None, skiprows=1)
    return cartoesCsv

def createCartaoDatabaseIfNotExist(cursor):
    colNames = ("partida_id", "rodata", "clube", "cartao", "atleta", "num_camisa", "posicao", "minuto")
    table_name = "cartoes"
    cartoesCsv = returnCartoesCsv()
    
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            {colNames[0]} VARCHAR(100) NOT NULL,
            {colNames[1]} VARCHAR(100) NOT NULL,
            {colNames[2]} VARCHAR(100),
            {colNames[3]} VARCHAR(100),
            {colNames[4]} VARCHAR(100),
            {colNames[5]} VARCHAR(100),
            {colNames[6]} VARCHAR(100),
            {colNames[7]} VARCHAR(100) 
        )    
    """
    cursor.execute(create_table_sql)
    
def insertDatasInCartaoDatabasse(cursor):
    table_name = "cartoes"
    cartoesCsv = returnCartoesCsv()
    cartoesCsv.replace(np.nan, None, inplace=True)

    columns = [col.replace(" ", "_") for col in cartoesCsv.columns]
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    
    for _, row in cartoesCsv.iterrows():
        cursor.execute(insert_sql, tuple(row))

# Planilha de gols

def returnGolsCsv():
    pathGols = 'planilhas/campeonato-brasileiro-gols.csv'
    colNames = ("partida_id", "rodata", "clube", "atleta", "minuto", "tipo_de_gol")
    table_name = "gols"
    golsCsv= pd.read_csv(pathGols, names=colNames,header=None, skiprows=1)
    return golsCsv

def createGolsDatabaseIfNotExist(cursor):
    colNames = ("partida_id", "rodata", "clube", "atleta", "minuto", "tipo_de_gol")
    table_name = "gols"
    golsCsv = returnGolsCsv()
    
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            {colNames[0]} VARCHAR(100) NOT NULL,
            {colNames[1]} VARCHAR(100) NOT NULL,
            {colNames[2]} VARCHAR(100),
            {colNames[3]} VARCHAR(100),
            {colNames[4]} VARCHAR(100),
            {colNames[5]} VARCHAR(100)
        )    
    """
    cursor.execute(create_table_sql)

def insertDatasInGolsDatabasse(cursor):
    table_name = "gols"
    golsCsv = returnGolsCsv()
    golsCsv.replace(np.nan, None, inplace=True)

    columns = [col.replace(" ", "_") for col in golsCsv.columns]
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    
    for _, row in golsCsv.iterrows():
        cursor.execute(insert_sql, tuple(row))

def main():
    load_dotenv()
    conn = connectDatabase()
    
    if conn:
        createCartaoDatabaseIfNotExist(conn.cursor())
        insertDatasInCartaoDatabasse(conn.cursor())

        createGolsDatabaseIfNotExist(conn.cursor())
        insertDatasInGolsDatabasse(conn.cursor())

        conn.commit()
        conn.close()
        
main()