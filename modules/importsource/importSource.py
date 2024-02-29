from modules.Info.info import sftpInfo
from py7zr import SevenZipFile
import os 
import gnupg
import paramiko
from utils import utils

def getWithSFTP():
    rep=['SIVSS','ANAP','CNAM','CNSA','ERRD','FINESS','INSEE','SIICEA']
    hostname,username,passphrase=sftpInfo()
    paramiko.util.log_to_file("paramiko.log")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname,username=username,password=passphrase)
    sftp = ssh.open_sftp()
    paths=utils.get_paths("settings/settings_demo.json","import")
    for r in rep[1:]:
        print(r)
        files=sftp.listdir_attr(paths["remotepath_input"]+r)
        for item in files:
            local_path=paths["localpath_input"]+r.lower()+'/'+item.filename
            remote_path=paths["remotepath_input"]+r+'/'+item.filename
            try:
                sftp.get(remote_path,local_path)
                print(local_path)
            except IOError as e:
                print(e)
    files=sftp.listdir_attr(paths["remotepath_sivss"])
    for item in files:
        local_path=paths["localpath_sivss"]+item.filename
        remote_path=paths["remotepath_sivss"]+item.filename
        if item.filename.split('_')[1] == 'Extraction':
            try:
                sftp.get(remote_path,local_path)
                print(local_path)
            except IOError as e:
                print(e)
    sftp.close()
    return

def decryptFile(filetype):
    print("decrypt file")
    getWithSFTP()
    paths=utils.get_paths("settings/settings_demo.json","import")
    dir_=paths["dir"]
    files=os.listdir(dir_)
    gpg=gnupg.GPG()
    for f in files:
        name=dir_+f
        if f.split('.')[-1]=='7z' and filetype=='7z':
            with SevenZipFile(name, mode='r') as z:
                print(f,z.needs_password())
                z.extractall()
                print(f+' est décompressé')
        if f.split('.')[-1]=='gpg' and filetype=='gpg':
            with open(name,'rb') as f_in:
                with open(name.split('.gpg')[0],'wb') as f_out:
                    status =gpg.decrypt_file(f_in,output=f_out)
                    
            print(f+' '+status.status)
    return 



