# -*- coding: utf-8 -*-
"""
Created on Tue Apr 11 10:38:45 2023

@author: mathieu.olivier
"""
import json
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


