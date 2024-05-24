import json
import csv
from processamento_dados import Dados 


def size_data(dados):
    return len(dados)

def join(dadosA, dadosB):
    combined_list = []
    combined_list.extend(dadosA)
    combined_list.extend(dadosB)
    return combined_list

def transformando_dados_tabela(dados, nomes_colunas):
    dados_combinados_tabela = [nomes_colunas]
    for row in dados:
        linha = []
        for coluna in nomes_colunas:
            linha.append(row.get(coluna, 'indisponivel'))
        dados_combinados_tabela.append(linha)
    return dados_combinados_tabela

def salvando_dados(dados, path):
    with open (path, 'w') as file:
        writer = csv.writer(file)
        writer.writerows(dados)      


path_csv = 'data_raw/dados_empresaB.csv'
path_json = 'data_raw/dados_empresaA.json'
path_dados_combinados = 'data_processed/dados_combinados.csv'

#Extract

dados_empresaA = Dados(path_json, 'json')
print(f'Nome colunas Empresa A{dados_empresaA.nome_colunas}')

dados_empresaB = Dados(path_csv, 'csv')
print(f'Nome colunas Empresa B{dados_empresaB.nome_colunas}')

#Transform

key_mapping = {'Nome do Item': 'Nome do Produto',
                'Classificação do Produto': 'Categoria do Produto',
                'Valor em Reais (R$)': 'Preço do Produto (R$)',
                'Quantidade em Estoque': 'Quantidade em Estoque',
                'Nome da Loja': 'Filial',
                'Data da Venda': 'Data da Venda'}

dados_empresaB.rename_columns(key_mapping)

print(f'Nome colunas Empresa B após transformação:{dados_empresaB.nome_colunas}')