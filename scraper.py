
# -*- coding: utf-8 -*-
import os
import shutil
from datetime import datetime

import pandas as pd
import requests
import scraperwiki
from sqlalchemy import create_engine

# morph.io requires this db filename, but scraperwiki doesn't nicely
# expose a way to alter this. So we'll fiddle our environment ourselves
# before our pipeline modules load.
os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'


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
    url = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv'
    file_name = url.split('/')[-1]
    file_path = os.path.join('downloads', file_name)
    # faz o download do arquivo na pasta
    # if download_file(url, file_path):
    processa_arquivo(file_path)

    return True


def processa_arquivo(file_path):
    try:
        url = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv'
        df = pd.read_csv(url, sep=';', encoding='latin1')
    except Exception as e:
        print('Erro ao ler arquivo', file_path, e)
        return False

    # transforma o campo saldo em número
    print(df.columns)

    df['CNPJ_CIA'] = df['CNPJ_CIA'].astype(str)
    df['DENOM_SOCIAL'] = df['DENOM_SOCIAL'].astype(str)
    df['DENOM_COMERC'] = df['DENOM_COMERC'].astype(str)
    df['DT_REG'] = df['DT_REG']
    df['DT_CONST'] = df['DT_CONST']
    df['DT_CANCEL'] = df['DT_CANCEL']
    df['MOTIVO_CANCEL'] = df['MOTIVO_CANCEL'].astype(str)
    df['SIT'] = df['SIT'].astype(str)
    df['DT_INI_SIT'] = df['DT_INI_SIT']
    df['CD_CVM'] = df['CD_CVM'].astype(int)
    df['SETOR_ATIV'] = df['SETOR_ATIV'].astype(str)
    df['TP_MERC'] = df['TP_MERC'].astype(str)
    df['CATEG_REG'] = df['CATEG_REG'].astype(str)
    df['DT_INI_CATEG'] = df['DT_INI_CATEG']
    df['SIT_EMISSOR'] = df['SIT_EMISSOR'].astype(str)
    df['DT_INI_SIT_EMISSOR'] = df['DT_INI_SIT_EMISSOR']
    df['TP_ENDER'] = df['TP_ENDER'].astype(str)
    df['LOGRADOURO'] = df['LOGRADOURO'].astype(str)
    df['COMPL'] = df['COMPL'].astype(str)
    df['BAIRRO'] = df['BAIRRO'].astype(str)
    df['MUN'] = df['MUN'].astype(str)
    df['UF'] = df['UF'].astype(str)
    df['PAIS'] = df['PAIS'].astype(str)
    df['CEP'] = df['CEP'].astype(str)
    df['DDD_TEL'] = df['DDD_TEL'].astype(str)
    df['TEL'] = df['TEL'].astype(str)
    df['DDD_FAX'] = df['DDD_FAX'].astype(str)
    df['FAX'] = df['FAX'].astype(str)
    df['EMAIL'] = df['EMAIL'].astype(str)
    df['TP_RESP'] = df['TP_RESP'].astype(str)
    df['RESP'] = df['RESP'].astype(str)
    df['DT_INI_RESP'] = df['DT_INI_RESP']
    df['LOGRADOURO_RESP'] = df['LOGRADOURO_RESP'].astype(str)
    df['COMPL_RESP'] = df['COMPL_RESP'].astype(str)
    df['BAIRRO_RESP'] = df['BAIRRO_RESP'].astype(str)
    df['MUN_RESP'] = df['MUN_RESP'].astype(str)
    df['UF_RESP'] = df['UF_RESP'].astype(str)
    df['PAIS_RESP'] = df['PAIS_RESP'].astype(str)
    df['CEP_RESP'] = df['CEP_RESP'].astype(str)
    df['DDD_TEL_RESP'] = df['DDD_TEL_RESP'].astype(str)
    df['TEL_RESP'] = df['TEL_RESP'].astype(str)
    df['DDD_FAX_RESP'] = df['DDD_FAX_RESP'].astype(str)
    df['FAX_RESP'] = df['FAX_RESP'].astype(str)
    df['EMAIL_RESP'] = df['EMAIL_RESP'].astype(str)
    df['CNPJ_AUDITOR'] = df['CNPJ_AUDITOR'].astype(str)
    df['AUDITOR'] = df['AUDITOR'].astype(str)

    df['DT_REF'] = datetime.today().strftime('%Y-%m-%d')

    # transforma o campo CNPJ_CIA e CNPJ_AUDITOR
    df['CNPJ_CIA'] = df['CNPJ_CIA'].str.replace('.', '')
    df['CNPJ_CIA'] = df['CNPJ_CIA'].str.replace('/', '')
    df['CNPJ_CIA'] = df['CNPJ_CIA'].str.replace('-', '')
    df['CNPJ_CIA'] = df['CNPJ_CIA'].str.zfill(14)

    df['CNPJ_AUDITOR'] = df['CNPJ_AUDITOR'].str.replace('.', '')
    df['CNPJ_AUDITOR'] = df['CNPJ_AUDITOR'].str.replace('/', '')
    df['CNPJ_AUDITOR'] = df['CNPJ_AUDITOR'].str.replace('-', '')
    df['CNPJ_AUDITOR'] = df['CNPJ_AUDITOR'].str.zfill(14)

    # remove os caracteres em brancos do nome das colunas
    df.rename(columns=lambda x: x.strip(), inplace=True)

    engine = create_engine('sqlite:///data.sqlite', echo=True)
    sqlite_connection = engine.connect()
    print('Importando usando pandas to_sql')
    df.to_sql(
        'swdata',
        sqlite_connection,
        if_exists='append',
        index=False
    )

    # for row in df.to_dict('records'):
    #     scraperwiki.sqlite.save(
    #         unique_keys=df.columns.values.tolist(), data=row)

    print('{} Registros importados com sucesso'.format(len(df)))

    return True


if __name__ == '__main__':
    main()

    # rename file
    print('Renomeando arquivo sqlite')
    if os.path.exists('scraperwiki.sqlite'):
        shutil.copy('scraperwiki.sqlite', 'data.sqlite')
