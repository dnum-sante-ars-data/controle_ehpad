# -*- coding: utf-8 -*-
"""
Created on Tue Jan 17 10:30:45 2023

@author: mathieu.olivier
"""

# Ajouter les fonctions qui permettent de préciser une commande pour run certaines fonctions seulement

# Modules à installer
import argparse
import re
from os import listdir
import pandas as pd
from modules.init_db.init_db import initDb, importSrcData, connDb
from utils import utils
from modules.transform.transform import executeTransform,inittable
from modules.export.export import localToSFTP
from modules.importsource.importSource import decryptFile

def __main__(args):
    if args.commande == "import":
        importData()
    elif args.commande == "create_csv":
        createCsv()
    elif args.commande == "init_database":
        exeDbInit()
    elif args.commande == "load_csv":
        loadCsvToDb()
    elif args.commande == "transform":
        if args.region is None:
               print("MERCI DE RENSEIGNER LA REGION SOUHAITEE. Si VOUS VOULEZ TOUTES LES REGIONS VEUILLEZ METTRE 0")
        elif  args.region == 0:
            list_region = utils.read_settings('settings/settings_demo.json',"region","code")
            for r in list_region:
                transform(r)
        else:
            transform(args.region) 
    elif args.commande == "export":
        if args.region is None:
               print("MERCI DE RENSEIGNER LA REGION SOUHAITEE. Si VOUS VOULEZ TOUTES LES REGIONS VEUILLEZ METTRE 0")
        elif  args.region == 0:
            list_region = utils.read_settings('settings/settings_demo.json',"region","code")
            for r in list_region:
                createExport(r)
        else:
            createExport(args.region)
    elif args.commande == "all":
        allFunctions(args.region)  
    return


def exeDbInit():
    # Opening JSON file
    dbname = utils.read_settings('settings/settings_demo.json',"db","name")
    initDb(dbname)
    return

def createCsv():
    # Go in all the input folders and store a csv clean version in to_csv
    allFolders = listdir('data/input')
    allFolders.remove('sivss')
    utils.concatSignalement()
    for folderName in allFolders:
        print("loop entrance")
        folderPath = 'data/input/{}'.format(folderName)
        allFiles =  listdir(folderPath)
        for inputFileName in allFiles:
            inputFilePath = folderPath+'/'+inputFileName
            outputFilePath = 'data/to_csv/'+inputFileName.split('.')[0]+'.csv'
            if re.search('demo.csv|demo.xlsx', inputFileName):
                print('file demo not added')
            elif inputFileName.split('.')[-1].lower()=='xlsx':
                print(inputFileName)
                utils.convertXlsxToCsv(inputFilePath,outputFilePath)
                print('converted excel file and added: {}'.format(inputFileName))
            elif inputFileName.split('.')[-1].lower()=='csv':
                outputExcel = inputFilePath.split('.')[0]+'.xlsx'
                df = pd.read_csv(inputFilePath, sep=';', encoding='latin-1', low_memory=(False))
                df.to_excel(outputExcel, encoding='UTF-8')
                df2 = pd.read_excel(outputExcel)
                df2.to_csv(outputFilePath, index = None, header=True, sep=';', encoding='UTF-8')
                print('added csv file: {}'.format(inputFileName))
                #shutil.copyfile(inputFilePath,outputFilePath)
    return

def loadCsvToDb():
    dbname = utils.read_settings('settings/settings_demo.json',"db","name")
    allCsv = listdir('data/to_csv')
    conn = connDb(dbname)
    for inputCsvFilePath in allCsv:
        importSrcData(
            utils.cleanSrcData(
                utils.csvReader( 'data/to_csv/'+inputCsvFilePath
                           )
                ),
            inputCsvFilePath.split('/')[-1].split('.')[0],
            conn,
            dbname
            )
        print("file added to db: {}".format(inputCsvFilePath))
    inittable()   
    return

def importData():
    print("import with SFTP")
    decryptFile()
    return



def transform(region):
    executeTransform(region)
    return 

def createExport(region):
    localToSFTP(region)
    return


def allFunctions(region):
    
    exeDbInit()
    createCsv()
    loadCsvToDb()
    if region == 0:
        list_region = utils.read_settings('settings/settings_demo.json',"region","code")
        for r in list_region:
            transform(r)
            createExport(r)
    else:
        createExport(region)
    return


# Initialisation du parsing
parser = argparse.ArgumentParser()
parser.add_argument("commande", type=str, help="Commande à exécuter")
parser.add_argument("region", type=int, help="Code region pour filtrer")
args = parser.parse_args()

# Core
if __name__ == "__main__":
    __main__(args)


