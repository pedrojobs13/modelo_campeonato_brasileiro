import pandas as pd
import psycopg2 as postgres
import os
from dotenv import load_dotenv
import numpy as np

 
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

# Planilha de cartões
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


# Planilha full
def returnFullCsv():
    pathFull = 'planilhas/campeonato-brasileiro-full.csv'
    colNames = ("ID", "rodata", "data","hora","mandante","visitante","formacao_mandante","formacao_visitante", 
                "tecnico_visitante", "vencedor","arena","mandante_Placar","visitante_Placar","mandante_Estado","visitante_Estado")
    table_name = "dados_completo"
    fullCsv= pd.read_csv(pathFull, names=colNames,header=None, skiprows=1)
    return fullCsv

def createFullDatabaseIfNotExist(cursor):
    colNames = ("ID", "rodata", "data","hora","mandante","visitante","formacao_mandante","formacao_visitante", "tecnico_mandante",
                "tecnico_visitante", "vencedor","arena","mandante_Placar","visitante_Placar","mandante_Estado","visitante_Estado")
    table_name = "dados_completo"
    fullCsv = returnFullCsv()
    
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id_database SERIAL PRIMARY KEY,
            {colNames[0]} VARCHAR(100) NOT NULL,
            {colNames[1]} VARCHAR(100) NOT NULL,
            {colNames[2]} VARCHAR(100),
            {colNames[3]} VARCHAR(100),
            {colNames[4]} VARCHAR(100),
            {colNames[5]} VARCHAR(100),
            {colNames[6]} VARCHAR(100),
            {colNames[7]} VARCHAR(100),
            {colNames[8]} VARCHAR(100),
            {colNames[9]} VARCHAR(100),
            {colNames[10]} VARCHAR(100),
            {colNames[11]} VARCHAR(100),
            {colNames[12]} integer,
            {colNames[13]} integer,
            {colNames[14]} VARCHAR(100),
            {colNames[15]} VARCHAR(100)
        )    
    """
    cursor.execute(create_table_sql)

def insertDatasInFullDatabasse(cursor):
    table_name = "dados_completo"
    fullCsv = returnFullCsv()
    fullCsv.replace(np.nan, None, inplace=True)

    columns = [col.replace(" ", "_") for col in fullCsv.columns]
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

    for _, row in fullCsv.iterrows():
        cursor.execute(insert_sql, tuple(row))

#Planilha estatisticas
def returnEstatisticasCsv():
    pathestatistica = 'planilhas/campeonato-brasileiro-estatisticas-full.csv'
    colNames = ("partida_id", "rodata", "clube", "chutes", "chutes_no_alvo", "posse_de_bola", "passes", 
                "precisao_passes", "faltas", "cartao_amarelo", "cartao_vermelho", "impedimentos", "escanteios")
    table_name = "estatistica"
    estatisticaCsv= pd.read_csv(pathestatistica, names=colNames,header=None, skiprows=1)
    return estatisticaCsv

def createEstatisticasDatabaseIfNotExist(cursor):
    colNames = ("partida_id", "rodata", "clube", "chutes", "chutes_no_alvo", "posse_de_bola", "passes", 
                "precisao_passes", "faltas", "cartao_amarelo", "cartao_vermelho", "impedimentos", "escanteios")
    table_name = "estatistica"
    estatisticaCsv = returnEstatisticasCsv()
    
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            {colNames[0]} VARCHAR(100) NOT NULL,
            {colNames[1]} VARCHAR(100) NOT NULL,
            {colNames[2]} VARCHAR(100),
            {colNames[3]} integer,
            {colNames[4]} integer,
            {colNames[5]} integer,
            {colNames[6]} integer,
            {colNames[7]} integer,
            {colNames[8]} integer,
            {colNames[9]} integer,
            {colNames[10]} integer,
            {colNames[11]} integer,
            {colNames[12]} integer
        )    
    """
    cursor.execute(create_table_sql)

def insertDatasInEstatisticasDatabasse(cursor):
    table_name = "estatistica"
    estatisticaCsv = returnEstatisticasCsv()
    estatisticaCsv.replace(np.nan, None, inplace=True)

    estatisticaCsv['precisao_passes'] = estatisticaCsv['precisao_passes'].str.replace(r'%', '', regex=True)
    estatisticaCsv['posse_de_bola'] = estatisticaCsv['posse_de_bola'].str.replace(r'%', '', regex=True)
    
    columns = [col.replace(" ", "_") for col in estatisticaCsv.columns]
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    
    for _, row in estatisticaCsv.iterrows():

        cursor.execute(insert_sql, tuple(row))

def main():
    load_dotenv()
    conn = connectDatabase()
    
    if conn:
        createCartaoDatabaseIfNotExist(conn.cursor())
        insertDatasInCartaoDatabasse(conn.cursor())

        createGolsDatabaseIfNotExist(conn.cursor())
        insertDatasInGolsDatabasse(conn.cursor())

        createFullDatabaseIfNotExist(conn.cursor())
        insertDatasInFullDatabasse(conn.cursor())

        createEstatisticasDatabaseIfNotExist(conn.cursor())
        insertDatasInEstatisticasDatabasse(conn.cursor())

        conn.commit()
        conn.close()

main()