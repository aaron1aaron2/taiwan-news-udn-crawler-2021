  
# encoding: utf-8
"""
@author: yen-nan ho
@contact: aaron1aaron2@gmail.com
@gitHub: https://github.com/aaron1aaron2
@Create Date: 2021/3/10
"""
import re
import os
import pandas as pd

file_folder = r"output\output0309"
output_folder = "output/final_output0309"
output_folder_all = "output/final_output0309/all"

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

if not os.path.exists(output_folder_all):
    os.makedirs(output_folder_all)

file_ls = [i for i in os.listdir(file_folder) if i.find('record')==-1]

total = 0
for file in file_ls:
    df = pd.read_csv(os.path.join(file_folder, file))
    total+=df.shape[0]

    extract = lambda x: re.sub(r"、、、+", "", re.sub(r"[A-Za-z0-9\!\%\[\]\,-\{\}\~\:\.\_\*'() （’–+#\"]", "", x))
    df.loc[~df['CONTENT'].isna(), 'CONTENT'] = df.loc[~df['CONTENT'].isna(), 'CONTENT'].apply(extract)

    df['CONTENT'] = df['CONTENT'].str.strip()

    df.to_csv(os.path.join(output_folder_all, file.replace('.udn', '.udn.all')))
    
    cols = ['TITLE', 'TIME', 'CATEGORY', 'HashTag', 'CONTENT' ,'SOURCE', 'LINK']
    df[cols].to_excel(os.path.join(output_folder, file.replace('.udn.csv', '.udn.xlsx')), index=False)

# total: 42861