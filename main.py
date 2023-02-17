# -*- coding: utf-8 -*-
"""
Created on Tue Jan 17 10:30:45 2023

@author: mathieu.olivier
"""


# Modules à installer
import os
from os import listdir
import pandas as pd 

#Regle à respecter fichier excel seulement
#La feuille d'interet doit etre placée en premier
#Les noms de colonne doivent être ne première ligne et aucune colonne ne doit être vide sur la gauche


def checkIfPathExists(file):
    if os.path.exists(file):
        os.remove(file)
        print('Ancien fichier écrasé')
        

def _convertXlsxToCsv(inputExcelFilePath, outputCsvFilePath):
    try:
    # Reading an excel file
    #   sheetname = getSheetName()
        excelFile = pd.read_excel(inputExcelFilePath, header=0)
        checkIfPathExists(outputCsvFilePath)
    # Converting excel file into CSV file
        excelFile.to_csv(outputCsvFilePath, index = None, header=True, sep=';', encoding='UTF-8')
        return outputCsvFilePath
    except ValueError as err:
        print(err)
        return str(err) 

#_convertXlsxToCsv("C:/Users/mathieu.olivier/Documents/Helios/Script_V2/input/Calcul du nombre de signalements.xlsx")

def _csvReader(csvFilePath):
    df = pd.read_csv(csvFilePath, sep= ';', encoding='UTF-8',low_memory=False)
    return df

#Pousser le csv sans mettre en dataframe



### Partie nettoyage des données

from unidecode import unidecode
import re

def _cleanTxt(text):
    try:
        text = unicode(text.lower(), 'utf-8')
    except (TypeError, NameError): # unicode is a default on python 3 
        pass
    text = unidecode(text.lower())
    text = text.encode('ascii', 'ignore')
    text = text.decode("utf-8")

    text = re.sub('[ ]+', '_', text)
    text = re.sub('[^0-9a-zA-Z_-]', '', text) 
    return str(text)

def _cleanSrcData(df):
# Enlever caractères spéciaux, accents, espace ( _ ) ,
    df.columns = [ _cleanTxt(i) for i in df.columns.values.tolist()]
    return df




### Partie Création de la DB et ajout des tables

import sqlite3
import shutil

dbname = 'data/database/controle_ehpad'

def checkIfDBExists(dbname):
    if os.path.exists(dbname + '.sqlite'):
        os.remove(dbname + '.sqlite')
        print('Ancienne base de donnée écrasée')

def _initDb(dbname):
    #Supprime l'ancienne base de donnée
    checkIfDBExists(dbname)
    #Crée la nouvelle base de donnée
    conn = sqlite3.connect(dbname + '.sqlite')
    conn
    print('Création de la base de donnée {}.sqlite '.format(dbname))
    return conn

conn = _initDb(dbname)

def _importSrcData(df, table_name):
    df.to_sql(name=table_name, con=conn)
    print('La table {} a été ajouté à la base de donnée {}'.format(table_name,dbname))
    return 

def _executeTransform():
    #Appeler les requetes sql
    return 

# Go in all the input folders and store a csv clean version in to_csv
allFolders = listdir('data/input')
#allFolders.remove('data/to_csv') => inutile car on a viré to_csv de input pour le mettre à part dans data

for folderName in allFolders:
    print("loop entrance")
    folderPath = 'data/input/{}'.format(folderName)
    allFiles =  listdir(folderPath)
    for inputFileName in allFiles:
        inputFilePath = folderPath+'/'+inputFileName
        outputFilePath = 'data/to_csv/'+inputFileName.split('.')[0]+'.csv'
        if inputFileName.split('.')[-1].lower()=='xlsx':
            _convertXlsxToCsv(inputFilePath,outputFilePath)
            print('converted excel file and added: {}'.format(inputFileName))
        elif inputFileName.split('.')[-1].lower()=='csv':
            df = pd.read_csv(inputFilePath, sep=';', encoding='latin-1')
            df.to_csv(outputFilePath, index = None, header=True, sep=';', encoding='UTF-8')
            print('added csv file: {}'.format(inputFileName))
            #shutil.copyfile(inputFilePath,outputFilePath)

allCsv = listdir('data/to_csv')



for inputCsvFilePath in allCsv:
    _importSrcData(
        _cleanSrcData(
            _csvReader( 'data/to_csv/'+inputCsvFilePath
                       )
            ),
        inputCsvFilePath.split('/')[-1].split('.')[0]
        )
    print("file added to db: {}".format(inputCsvFilePath))
    
conn.close()

#%% Update section
#Pour Update une table à partir d'un fichier
# Ca ne marche pas pour l'instant 
# En pause car pas nécessaire

def _updateTable(dbname, table_name, excelFilePath):
    try:
        sqliteConnection = sqlite3.connect(dbname+'.sqlite')
        cursor = sqliteConnection.cursor()
        print("Connected to {}".format(dbname))

        #sql_drop_table = "DROP TABLE {}".format(table_name)
        cursor.execute("DROP TABLE "+table_name)
        sqliteConnection.commit()
        print("Table {} dropped".format(table_name))
        _importSrcData(
            _cleanSrcData(
                _csvReader(
                    _convertXlsxToCsv(excelFilePath)
                )
            ),
            table_name
            )
        print("Table {} Updated".format(table_name))
        cursor.close()

    except sqlite3.Error as error:
        print("Failed to update sqlite table", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("The SQLite connection is closed")
table_name = '20220624_Diamant_MS_Demande_BD'
_updateTable(dbname, table_name, "input/sources/20220624_Diamant_MS_Demande_BD.xlsx" )

'''
Sources: 
    - input_xslx
    - input_csx
    - ref
'''

