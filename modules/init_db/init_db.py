# -*- coding: utf-8 -*-
"""
Created on Mon Fev 20 14:38:45 2023

@author: mathieu.olivier
"""
import pandas as pd
import sqlite3
import os


# à déplacer dans modules/init_db/init_db.py
### Partie Création de la DB et ajout des tables

def checkIfDBExists(dbname):
    if os.path.exists(dbname + '.sqlite'):
        os.remove(dbname + '.sqlite')
        print('Ancienne base de donnée écrasée')

def initDb(dbname):
    #Supprime l'ancienne base de donnée
    checkIfDBExists(dbname)
    #Crée la nouvelle base de donnée
    conn = sqlite3.connect(dbname + '.sqlite')
    conn
    print('Création de la base de donnée {}.sqlite '.format(dbname))
    return conn

def connDb(dbname):
    conn = sqlite3.connect(dbname + '.sqlite')
    conn
    return conn

def importSrcData(df, table_name, conn, dbname):
    df.to_sql(name=table_name, con=conn)
    print('La table {} a été ajouté à la base de donnée {}'.format(table_name,dbname))
    return 
