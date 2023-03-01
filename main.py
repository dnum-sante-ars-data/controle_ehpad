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
from modules.init_db.init_db import _initDb, _importSrcData
from utils import utils
import json

def __main__(args):
    if args.commande == "create_csv":
        _createCsv()
    elif args.commande == "init_database":
        _exeDbInit()
    elif args.commande == "load_csv":
        _loadCsvToDb()
    elif args.commande == "all":
        _allFunctions()  
    elif args.commande == "rien":
        print('rien') 
    return


def _exeDbInit():
    # Opening JSON file
    f = open('settings/settings_demo.json')
      
    # returns JSON object as 
    # a dictionary
    data = json.load(f)
    print(data)
    _initDb('data/database/controle_ehpad')
    return

def _createCsv():
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
            if inputFileName == 'demo.csv' or inputFileName == 'demo.xlsx':
                print('file demo')
            elif inputFileName.split('.')[-1].lower()=='xlsx':
                utils._convertXlsxToCsv(inputFilePath,outputFilePath)
                print('converted excel file and added: {}'.format(inputFileName))
            elif inputFileName.split('.')[-1].lower()=='csv':
                df = pd.read_csv(inputFilePath, sep=';', encoding='latin-1')
                df.to_csv(outputFilePath, index = None, header=True, sep=';', encoding='UTF-8')
                print('added csv file: {}'.format(inputFileName))
                #shutil.copyfile(inputFilePath,outputFilePath)
    return


def _loadCsvToDb():
    allCsv = listdir('data/to_csv')
    for inputCsvFilePath in allCsv:
        _importSrcData(
            utils._cleanSrcData(
                utils._csvReader( 'data/to_csv/'+inputCsvFilePath
                           )
                ),
            inputCsvFilePath.split('/')[-1].split('.')[0]
            )
        print("file added to db: {}".format(inputCsvFilePath))
    return

def _allFunctions():
    _exeDbInit()
    _createCsv()
    _loadCsvToDb()
    return

# Initialisation du parsing
parser = argparse.ArgumentParser()
parser.add_argument("commande", type=str, help="Commande à exécuter")
args = parser.parse_args()

# Core
if __name__ == "__main__":
    __main__(args)


