# -*- coding: utf-8 -*-
"""
Created on Tue Apr 11 10:38:45 2023

@author: mathieu.olivier
"""
import json
import pysftp

def sftpInfo():
    with open('settings/settings_demo.json') as f:
        # Load the JSON data from the file
        data = json.load(f)
    # Find the index of the code equal to 2 in the "code" list
    url = data["sftp"][0]["url"]
    username = data["sftp"][0]["username"]
    passphrase =  data["sftp"][0]["passphrase"]
    return url, username, passphrase



# Informations de connexion SFTP
hostname, username, passphrase = sftpInfo()
port = 22

# Créer une instance de l'objet SFTP
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None  # Ignorer la vérification de l'empreinte du serveur (uniquement pour les tests de développement)
sftp = pysftp.Connection(hostname, username=username, password=passphrase)

'''
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
'''

# Fermer la connexion SFTP
sftp.close()


