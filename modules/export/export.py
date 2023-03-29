# -*- coding: utf-8 -*-
"""
Created on Wed Mar 22 10:38:45 2023

@author: mathieu.olivier
"""

import pandas as pd
from datetime import datetime



def _export(region, df_ciblage, df_controle):
    print("début de la création de l'export")
    date_string = datetime.today().strftime('%d%m%Y') 
    path = 'data/output/export_{}_{}.xlsx'.format(region,date_string)
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(path, engine='xlsxwriter')
    df_ciblage.to_excel(writer, sheet_name='ciblage', index=False)
    df_controle.to_excel(writer, sheet_name='controle', index=False)
    # Close the Pandas Excel writer and output the Excel file.
    writer.close()
    print('export créé : export_{}_{}.xlsx'.format(region,date_string))
    return

