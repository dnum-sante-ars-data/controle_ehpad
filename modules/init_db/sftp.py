# -*- coding: utf-8 -*-
"""
Created on Tue Apr 11 10:38:45 2023

@author: mathieu.olivier
"""
import json
import paramiko
from modules.export.export import _outputName
from datetime import datetime

def sftpInfo():
    with open('settings/settings_demo.json') as f:
        # Load the JSON data from the file
        data = json.load(f)
    # Find the index of the code equal to 2 in the "code" list
    url = data["sftp"][0]["url"]
    username = data["sftp"][0]["username"]
    passphrase =  data["sftp"][0]["passphrase"]
    print('{}, {}'.format(url,username))
    return url, username, passphrase





# Do some operations with the SFTP client...
# 
# client.get('remote_filename', 'local_filename')
def excelToSFTP(region):
    # Informations de connexion SFTP
    hostname, username, passphrase = sftpInfo()
    paramiko.util.log_to_file("paramiko.log")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print('création du client')
    ssh.connect(hostname, username=username, password=passphrase)
    # Create an SFTP client object
    #client = paramiko.SFTPClient.from_transport(
    #    paramiko.Transport((hostname, 22))
    #)
    print('Ouverture de la connexion SFTP')
    # Authenticate with the server
    #client.connect(username=username, password=passphrase)
    date_string = datetime.today().strftime('%d%m%Y') 
    localpath = 'data/output/{}_{}.xlsx'.format(_outputName(region),date_string)
    remotepath = '/SCN_BDD/SIREC/{}_{}.xlsx'.format(_outputName(region),date_string)
    sftp = ssh.open_sftp()
    print('sftp open')
    sftp.put(localpath, remotepath)
    #client.put(local_path, remote_path)
    print('Fichier {}_{}.xlsx déposé en dans /SCN_BDD/SIREC'.format(_outputName(region),date_string))
    # Close the SFTP client connection
    #client.close()
    print('Fermeture de la connexion SFTP')
    return 

 

'''
# Créer une instance de l'objet SFTP
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None  # Ignorer la vérification de l'empreinte du serveur (uniquement pour les tests de développement)
sftp = pysftp.Connection(hostname, username=username, password=passphrase)

# Effectuer des opérations SFTP (par exemple, télécharger un fichier)
sftp.get('fichier_distant.txt', 'fichier_local.txt')

# Lire le contenu d'un fichier distant
remote_file_path = '/chemin/vers/le/fichier_distant.txt'
with sftp.open(remote_file_path, 'r') as remote_file:
    content = remote_file.read()
    print(content)

# Déposer un fichier local sur le serveur SFTP
local_file_path = '/chemin/vers/le/fichier_local.txt'
remote_file_path = '/chemin/vers/le/fichier_distant.txt'
sftp.put(local_file_path, remote_file_path)


# Fermer la connexion SFTP
sftp.close()'''

def sftpToLocal():
    # Informations de connexion SFTP
    hostname, username, passphrase = sftpInfo()
    paramiko.util.log_to_file("paramiko.log")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print('création du client')
    ssh.connect(hostname, username=username, password=passphrase)
    # Create an SFTP client object
    #client = paramiko.SFTPClient.from_transport(
    #    paramiko.Transport((hostname, 22))
    #)
    print('Ouverture de la connexion SFTP')
    sftp = ssh.open_sftp()
    print('sftp open')
    path = "/SCN_BDD/"
    liste = ["ATIH","CNAM","CNSA","DIAMANT", "ERRD", "FINESS", "INSEE", "SICEA", "SIREC", "SIVSS"]
    localpath = 'data/output/{}_{}.xlsx'.format(_outputName(region),date_string)
    remotepath = '/SCN_BDD/SIREC/{}_{}.xlsx'.format(_outputName(region),date_string)
    sftp.put(localpath, remotepath)
    return

