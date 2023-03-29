# -*- coding: utf-8 -*-
"""
Created on Tue Jan 17 10:30:45 2023

@author: mathieu.olivier
"""

# Ajouter les fonctions qui permettent de préciser une commande pour run certaines fonctions seulement

# Modules à installer
import argparse
from os import listdir
import pandas as pd 
from modules.init_db.init_db import _initDb, _importSrcData, _connDb
from utils import utils
from modules.transform.transform import _executeTransform
from modules.export.export import _export

def __main__(args):
    if args.commande == "create_csv":
        _createCsv()
    elif args.commande == "init_database":
        _exeDbInit()
    elif args.commande == "load_csv":
        _loadCsvToDb()
    elif args.commande == "export":
        _createExport(region = args.region)
    elif args.commande == "all":
        _allFunctions()  
    return


def _exeDbInit():
    # Opening JSON file
    dbname = utils.read_settings('settings/settings_demo.json',"db","name")
    _initDb(dbname)
    return

def _createCsv():
    # Go in all the input folders and store a csv clean version in to_csv
    allFolders = listdir('data/input')
    allFolders.remove('sivss')
    utils._concatSignalement()
    for folderName in allFolders:
        print("loop entrance")
        folderPath = 'data/input/{}'.format(folderName)
        allFiles =  listdir(folderPath)
        for inputFileName in allFiles:
            inputFilePath = folderPath+'/'+inputFileName
            outputFilePath = 'data/to_csv/'+inputFileName.split('.')[0]+'.csv'
            if inputFileName == 'demo.csv' or inputFileName == 'demo.xlsx':
                print('file demo not added')
            elif inputFileName.split('.')[-1].lower()=='xlsx':
                utils._convertXlsxToCsv(inputFilePath,outputFilePath)
                print('converted excel file and added: {}'.format(inputFileName))
            elif inputFileName.split('.')[-1].lower()=='csv':
                outputExcel = inputFilePath.split('.')[0]+'.xlsx'
                df = pd.read_csv(inputFilePath, sep=';', encoding='latin-1', low_memory=(False))
                df.to_excel(outputExcel)
                df2 = pd.read_excel(outputExcel)
                df2.to_csv(outputFilePath, index = None, header=True, sep=';', encoding='UTF-8')
                print('added csv file: {}'.format(inputFileName))
                #shutil.copyfile(inputFilePath,outputFilePath)
    return


def _loadCsvToDb():
    dbname = utils.read_settings('settings/settings_demo.json',"db","name")
    allCsv = listdir('data/to_csv')
    conn = _connDb(dbname)
    for inputCsvFilePath in allCsv:
        _importSrcData(
            utils._cleanSrcData(
                utils._csvReader( 'data/to_csv/'+inputCsvFilePath
                           )
                ),
            inputCsvFilePath.split('/')[-1].split('.')[0],
            conn,
            dbname
            )
        print("file added to db: {}".format(inputCsvFilePath))
    return

def _createExport(region):
    df_ciblage, df_controle = _executeTransform(region)
    _export(region, df_ciblage, df_controle)
    return

def _allFunctions():
    _exeDbInit()
    _createCsv()
    _loadCsvToDb()
    _createExport()
    return

# Initialisation du parsing
parser = argparse.ArgumentParser()
parser.add_argument("commande", type=str, help="Commande à exécuter")
parser.add_argument("region", type=int, help="Code region pour filtrer")
args = parser.parse_args()

# Core
if __name__ == "__main__":
    __main__(args)


