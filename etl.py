import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *
from sqlalchemy import create_engine


def process_song_file(cur, filepath):
    """ A função lê/processa os arquivos de dados contendo músicas e os metadados para inserir nas tabelas.
 
    filepath é a localização do arquivo JSON.  
    """

    # open song file
    df = pd.read_json(filepath, lines=True)     # Leitura do arquivo por linha.

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()     # Convertendo um dataframe para uma lista.

    cur.execute(song_table_insert, song_data) # Inserindo

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist() # Convertendo um dataframe para uma lista.

    cur.execute(artist_table_insert, artist_data) # Inserindo


def process_log_file(cur, filepath):
    """A função lê/processa os arquivos de dados contendo os logs para inserir nas tabelas.
    """

    # open log file
    df = pd.read_json(filepath, lines=True) # Leitura do arquivo por linha.

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = np.transpose(np.array([df['ts'].values, t.dt.hour.values, t.dt.day.values, t.dt.week.values,
                                       t.dt.month.values, t.dt.year.values, t.dt.weekday.values])) # Transposição
    column_labels = ('ts', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(data=time_data, columns=column_labels) # Inserção

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row)) # Iteração por linha

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']] 

    # insert user records
    for i, row in user_df.drop_duplicates().iterrows():      # Inserindo dados e excluindo duplicatas
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # Buscando songid e artistid das tabelas song e artist, respectivamente.
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()    # Busca a próxima linha de dados ativa, retornando em uma única tupla e None depois da última linha lida.

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid,
                         row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    """Pega todos os arquivos no filepath e executa as funções process_song_file e/ou process_log_file.
    """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))    # glob é utilizado para manipular os arquivos e diretórios
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print(f'{num_files} arquivos encontrados em {filepath}')

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print(f'{i}/{num_files} arquivos processados.')


def main():
    """ Esse programa tem como função ler todos arquvivos de logs e músicas, para carregá-los nas tabelas de dimensão e fato.
    """

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.commit()

    conn.close()


if __name__ == "__main__":
    main()
