import pandas as pd


def select_brasil_municipios(input_data, uf='all'):
    """
    Etapa: Seleção
    Operação: Redução de Dados Vertical e Horizontal Direta
    :param input_data
    :param uf
    :return:
    """
    path = f'../data/{input_data}'
    columns = ['ibge_code', 'municipio', 'estado', 'mesorregiao', 'microrregiao']
    data = pd.read_csv(path)
    if uf == 'all':
        data = data.loc[:, columns].copy()
    else:
        data = data.loc[
            data['ibge_code'].astype('string').str.startswith(uf),
            columns
        ].copy()
    data.to_csv(f"../data/{input_data.split('.')[0]}_reduced_{uf}.csv", index=False, encoding='utf-8')
    print(f'seleção municípios: {data.shape}')
