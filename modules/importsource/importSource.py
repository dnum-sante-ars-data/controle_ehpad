from modules.init_db.sftp import getWithSFTP
from py7zr import SevenZipFile
import os
import gnupg

def getData(filetype):
    print("import Source")
    getWithSFTP()
    dir_='/mnt/test_my_scripts/controle_ehpad_v4/controle_ehpad/data/input/sivss/'
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
