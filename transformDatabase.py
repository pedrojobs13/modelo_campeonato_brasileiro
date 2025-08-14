import pandas as pd
import psycopg2 as postgres
import os
from dotenv import load_dotenv
import numpy as np
 
def connectDatabase():
    try: 
        conn = postgres.connect(database="brasileirao_star",
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

def main():
    load_dotenv()
    conn = connectDatabase()
    
    if conn:
        createTables(conn.cursor())
        conn.commit()
        conn.close()
main()