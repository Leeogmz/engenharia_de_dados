from processamento_dados import Dados 


path_csv = 'engenharia_de_dados/projeto_pipeline_dados/data_raw/dados_empresaB.csv'
path_json = 'engenharia_de_dados/projeto_pipeline_dados/data_raw/dados_empresaA.json'
path_dados_combinados = 'engenharia_de_dados/projeto_pipeline_dados/data_processed/dados_combinados.csv'

#Extract

dados_empresaA = Dados(path_json, 'json')
print(f'Nome colunas Empresa A: {dados_empresaA.nome_colunas}')
print(f'Quantidade de linhas Empresa A: {dados_empresaA.qtd_linhas}')

dados_empresaB = Dados(path_csv, 'csv')
print(f'Nome colunas Empresa B: {dados_empresaB.nome_colunas}')
print(f'Quantidade de linhas Empresa B: {dados_empresaB.qtd_linhas}')

#Transform

key_mapping = {'Nome do Item': 'Nome do Produto',
                'Classificação do Produto': 'Categoria do Produto',
                'Valor em Reais (R$)': 'Preço do Produto (R$)',
                'Quantidade em Estoque': 'Quantidade em Estoque',
                'Nome da Loja': 'Filial',
                'Data da Venda': 'Data da Venda'}

dados_empresaB.rename_columns(key_mapping)

print(f'Nome colunas Empresa B após transformação: {dados_empresaB.nome_colunas}')
print(f'Quantidade de linhas Empresa B após transformação: {dados_empresaB.qtd_linhas}')

dados_fusao = Dados.join(dados_empresaA, dados_empresaB)

print(dados_fusao)
print(f'Nome colunas após fusão: {dados_fusao.nome_colunas}')
print(f'Quantidade de linhas após fusão: {dados_fusao.qtd_linhas}')

#Load

path_dados_combinados = 'engenharia_de_dados/projeto_pipeline_dados/data_processed/dados_combinados.csv'
dados_fusao.salvando_dados(path_dados_combinados)
print(f'O arquivo processado foi salvo em: {path_dados_combinados}')