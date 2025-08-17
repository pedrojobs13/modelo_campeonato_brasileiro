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

def createTables(cursor):
     
    tables = (
        """
        CREATE TABLE IF NOT EXISTS dim_partida (
            id_partida_sk SERIAL PRIMARY KEY,
            partida_id_origem VARCHAR(100),
            descricao_partida VARCHAR(255),
            vencedor VARCHAR(100)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_tempo (
            id_tempo_sk SERIAL PRIMARY KEY,
            data_partida DATE,
            hora_partida TIME,
            rodada INT,
            dia INT,
            mes INT,
            ano INT,
            dia_da_semana VARCHAR(20)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_clube (
            id_clube_sk SERIAL PRIMARY KEY,
            nome_clube VARCHAR(100),
            estado_clube VARCHAR(100)
        )"""
        ,
        """
        CREATE TABLE IF NOT EXISTS dim_tecnico (
            id_tecnico_sk SERIAL PRIMARY KEY,
            nome_tecnico VARCHAR(100)
        )
        """
        ,
        """
        CREATE TABLE IF NOT EXISTS dim_arena (
            id_arena_sk SERIAL PRIMARY KEY,
            nome_arena VARCHAR(100)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS fato_desempenho_partida (
            id_partida_sk INT NOT NULL,
            id_tempo_sk INT NOT NULL,
            id_clube_sk INT NOT NULL,
            id_clube_adversario_sk INT NOT NULL,
            id_tecnico_sk INT NOT NULL,
            id_arena_sk INT NOT NULL,
            gols_marcados INT,
            gols_sofridos INT,
            chutes INT,
            chutes_no_alvo INT,
            posse_de_bola INT,
            passes INT,
            precisao_passes INT,
            faltas INT,
            cartoes_amarelos INT,
            cartoes_vermelhos INT,
            impedimentos INT,
            escanteios INT,
            resultado VARCHAR(10),        
            CONSTRAINT pk_fato PRIMARY KEY (
                id_partida_sk,
                id_tempo_sk,
                id_clube_sk,
                id_clube_adversario_sk,
                id_tecnico_sk,
                id_arena_sk
            ),
        CONSTRAINT fk_partida FOREIGN KEY (id_partida_sk) REFERENCES dim_partida (id_partida_sk),
        CONSTRAINT fk_tempo FOREIGN KEY (id_tempo_sk) REFERENCES dim_tempo (id_tempo_sk),
        CONSTRAINT fk_clube FOREIGN KEY (id_clube_sk) REFERENCES dim_clube (id_clube_sk),
        CONSTRAINT fk_clube_adversario FOREIGN KEY (id_clube_adversario_sk) REFERENCES dim_clube (id_clube_sk),
        CONSTRAINT fk_tecnico FOREIGN KEY (id_tecnico_sk) REFERENCES dim_tecnico (id_tecnico_sk),
        CONSTRAINT fk_arena FOREIGN KEY (id_arena_sk) REFERENCES dim_arena (id_arena_sk)
        )
        """,
    )
    
    for table in tables:
        cursor.execute(table)


def insertDim_partida(cursor):
    
    insert_table_sql = f""" 
    INSERT INTO dim_partida (partida_id_origem, descricao_partida, vencedor)
        SELECT DISTINCT
        id,
        CONCAT(mandante, ' vs ', visitante),
        -- Lógica para substituir o '-' por 'Empate'
        CASE
            WHEN vencedor = '-' THEN 'Empate'
            ELSE vencedor
        END AS vencedor_tratado
    FROM dados_completo
    """
    cursor.execute(insert_table_sql)

def insertDim_arena(cursor):
    
    insert_table_sql = f""" 
        INSERT INTO dim_arena (nome_arena)
        SELECT DISTINCT arena FROM dados_completo
    """
    cursor.execute(insert_table_sql)

def insertDim_tecnico(cursor):
    insert_table_sql = f""" 
        INSERT INTO dim_tecnico (nome_tecnico)
        SELECT DISTINCT tecnico_mandante FROM dados_completo
        UNION
        SELECT DISTINCT tecnico_visitante FROM dados_completo;
    """
    cursor.execute(insert_table_sql)

def insertDim_tempo(cursor):
    insert_table_sql = f""" 
    INSERT INTO dim_tempo (data_partida, hora_partida, rodada, dia, mes, ano, dia_da_semana)
        SELECT DISTINCT
            TO_DATE(data, 'DD/MM/YYYY'),
            TO_TIMESTAMP(hora, 'HH24:MI:SS')::TIME,
            CAST(rodata AS INTEGER),
            EXTRACT(DAY FROM TO_DATE(data, 'DD/MM/YYYY')),
            EXTRACT(MONTH FROM TO_DATE(data, 'DD/MM/YYYY')),
            EXTRACT(YEAR FROM TO_DATE(data, 'DD/MM/YYYY')),
            TO_CHAR(TO_DATE(data, 'DD/MM/YYYY'), 'Day')
    FROM dados_completo
    """
    cursor.execute(insert_table_sql)

def insertDim_clube(cursor):
    insert_table_sql = f""" 
        INSERT INTO dim_clube (nome_clube, estado_clube)
        SELECT DISTINCT mandante, mandante_estado FROM dados_completo
        UNION
        SELECT DISTINCT visitante, visitante_estado FROM dados_completo;
    """
    cursor.execute(insert_table_sql)  
      
def main():
    load_dotenv()
    conn = connectDatabase()

    if conn:
        createTables(conn.cursor())
        insertDim_partida(conn.cursor())
        insertDim_arena(conn.cursor())
        insertDim_tecnico(conn.cursor())
        insertDim_tempo(conn.cursor())
        insertDim_clube(conn.cursor())
        conn.commit()
        conn.close()
main()