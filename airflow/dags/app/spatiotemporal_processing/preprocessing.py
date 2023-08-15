from dbfread.dbf import DBF
import rpy2.robjects as ro
import pandas as pd
import numpy as np
import os


def convert_dbc_to_csv(input_data, output):
    """
    Descrição: Esta função executa o script em R usando rpy2.
    Etapa: Pré-Seleção
    Operação: Conversão de dados
    :param input_data:
    :param output:
    :return:
    """
    r = ro.r
    r.source('dbc_to_csv.r')
    r.dbc_to_csv(input_data, output)


def convert_dbf_to_csv(input_data, output):
    """
    Descrição: Converte raw data de .dbf para .csv
    Etapa: Pré-Seleção
    Operação: Conversão de Dados
    :param input_data:
    :param output:
    :return:
    """
    input_path = f'../data/{input_data}'
    output_path = f'../data/{output}'
    dbf_files = [it for it in os.listdir(input_path) if '.dbf' in it.lower()]
    for file in dbf_files:
        dbf = DBF(f'{input_path}/{file}')
        df = pd.DataFrame(dbf)
        df.to_csv(f'{output_path}/{file.split(".")[0]}_raw.csv', index=False)


def convert_leivis_columns_data_types(input, output):
    """
    Etapa: Codificação
    Operação: Conversão de tipos (string para integer e datetime).
    :param input:
    :param output:
    :return:
    """
    data = pd.read_csv(f'../data/{input}')
    dates_columns = [
        'TRATAMENTO',
        'DT_NOT',
        'DT_PRI_SIN',
        'DT_NASC',
        'DT_INVEST',
        'DT_OBITO',
        'DT_ENCERRAMENTO',
        'DT_DESLC1',
        'DT_DESLC2',
        'DT_DESLC3'
    ]
    to_int_columns = ['ANO', 'CO_MN_INF', 'CO_MN_RESI', 'CO_MN_NOT', 'CS_GESTANT', 'CS_RACA',
                      'ID_OCUPA_N', 'CS_ESCOL_N', 'CO_PAIS', 'FEBRE', 'FRAQUEZA',
                      'EDEMA', 'EMAGRECIMENTO', 'DROGA', 'EVOLUCAO', 'DOENCA_TRABALHO',
                      'FALENCIA', 'TOSSE', 'PALIDEZ', 'BACO', 'INFECCIOSO', 'FEN_HEMORR',
                      'FIGADO', 'ICTERICIA', 'OUTROS', 'HIV', 'DIAG_PARASITOLOGICO',
                      'IFI', 'OUTRO', 'ENTRADA', 'DOSE', 'AMPOLAS', 'CRITERIO',
                      'TPAUTOCTO', 'CO_UF_INF', 'DS_MUN_1', 'CO_UF_1', 'CO_PAIS_1',
                      'DS_MUN_2', 'CO_UF_2', 'CO_PAIS_2', 'DS_MUN_3', 'CO_UF_3',
                      'CO_PAIS_3']

    data[to_int_columns] = data[to_int_columns].astype(np.float).astype("Int32")
    data[dates_columns] = data[dates_columns].apply(lambda x: pd.to_datetime(x), axis=1)
    data.to_csv(f'../data/{output}', index=False, encoding='utf-8')


def concatenate_csv(input_data, output, drop_original=True):
    """
    Descrição: Lê e junta todos arquivos .csv em um dataframe.
    Etapa: Pré-Seleção
    Operação: União de bases de dados
    :param input_data:
    :param output:
    :param drop_original: Se True, substitui os arquivos individuais por um único .csv
    :return:
    """
    import errno

    def silent_remove(file):
        try:
            os.remove(file)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise

    input_path = f'../data/{input_data}'
    output_path = f'../data/{output}'
    files = os.listdir(input_path)
    files = [it for it in files if '_raw.csv' in it]
    df = pd.DataFrame()
    for filename in files:
        df = pd.concat([df, pd.read_csv(f'{input_path}/{filename}', encoding='latin1', low_memory=False)], axis=0)

    if drop_original:
        for filename in files:
            silent_remove(f'{input_path}/{filename}')
    df.to_csv(f"{output_path}/{output.replace('/', '_')}.csv", index=False)
