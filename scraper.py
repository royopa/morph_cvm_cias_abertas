
# -*- coding: utf-8 -*-
import os
from datetime import datetime
import pandas as pd
import shutil
import requests
import scraperwiki


def download_file(url, file_path):
    response = requests.get(url, stream=True)
    
    if response.status_code != 200:
        print('Arquivo não encontrado', url, response.status_code)
        return False

    with open(file_path, "wb") as handle:
        print('Downloading', url)
        for data in response.iter_content():
            handle.write(data)    
    handle.close()
    return True

    
def create_download_folder():
    # Create directory
    dirName = os.path.join('downloads')
 
    try:
        # Create target Directory
        os.mkdir(dirName)
        print("Directory", dirName, "Created ")
    except Exception:
        print("Directory", dirName, "already exists")


def main():
    create_download_folder()
    download_arquivo()
    return True


def download_arquivo():
    url = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/inf_cadastral_cia_aberta.csv'

    # morph.io requires this db filename, but scraperwiki doesn't nicely
    # expose a way to alter this. So we'll fiddle our environment ourselves
    # before our pipeline modules load.
    os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'

    file_name = url.split('/')[-1]
    file_path = os.path.join('downloads', file_name)
    # faz o download do arquivo na pasta       
    if download_file(url, file_path):
        processa_arquivo(file_path)

    return True


def processa_arquivo(file_path):
    try:
        df = pd.read_csv(file_path, sep=';', encoding='latin1')
    except Exception as e:
        print('Erro ao ler arquivo', file_path, e)
        return False

    # transforma o campo saldo em número
    print(df.columns)

    df['DT_REF'] = datetime.today().strftime('%Y-%m-%d')

    # transforma o campo CNPJ_CIA e CNPJ_AUDITOR
    df['CNPJ_CIA'] = df['CNPJ_CIA'].str.replace('.','')
    df['CNPJ_CIA'] = df['CNPJ_CIA'].str.replace('/','')
    df['CNPJ_CIA'] = df['CNPJ_CIA'].str.replace('-','')
    df['CNPJ_CIA'] = df['CNPJ_CIA'].str.zfill(14)

    df['CNPJ_AUDITOR'] = df['CNPJ_AUDITOR'].str.replace('.','')
    df['CNPJ_AUDITOR'] = df['CNPJ_AUDITOR'].str.replace('/','')
    df['CNPJ_AUDITOR'] = df['CNPJ_AUDITOR'].str.replace('-','')
    df['CNPJ_AUDITOR'] = df['CNPJ_AUDITOR'].str.zfill(14)

    df['PAIS'] = df['PAIS'].astype(str)
    df['UF'] = df['UF'].astype(str)
    df['UF_RESP'] = df['UF_RESP'].astype(str)
    df['PAIS_RESP'] = df['PAIS_RESP'].astype(str)

    # remove os caracteres em brancos do nome das colunas
    df.rename(columns=lambda x: x.strip(), inplace=True)
   
    for row in df.to_dict('records'):
        scraperwiki.sqlite.save(unique_keys=df.columns, data=row)

    print('{} Registros importados com sucesso'.format(len(df)))

    return True


if __name__ == '__main__':
    main()

    # rename file
    print('Renomeando arquivo sqlite')
    if os.path.exists('scraperwiki.sqlite'):
        shutil.copy('scraperwiki.sqlite', 'data.sqlite')
