import polars as pl
from datetime import datetime
from matplotlib import pyplot as plt
import numpy as np


ENDERECO_DADOS = r'./dados/'

# LENDO OS DADOS DO ARQUIVO PARQUET
try:
    print('\nIniciando leitura do arquivo parquet...')

    # Pega o tempo inicial
    inicio = datetime.now()

    # Exemplo:
    # df_bolsa_familia = pl.read_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet') 
    
    # Scan_parquet: Geralment é um método mais rápido que read_parquet,
    # O Scan_parquet gera um plano de execução, para realizar a leitura dos dados.  
    df_bolsa_familia_plan = pl.scan_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')
    
    # collect() é o método que executa esse plano de execução.
    df_bolsa_familia = df_bolsa_familia_plan.collect()
    
    print(df_bolsa_familia)

    # Pega o tempo final
    fim = datetime.now()

    print(f'Tempo de execução para leitura do parquet: {fim - inicio}')
    print('\nArquivo parquet lido com sucesso!')

except ImportError as e: 
    print(f'Erro ao ler os dados do parquet: {e}')


# Visualizar a distribuição dos valores das parcelas em um boxplot
try:
    print('Visualizando a distribuição dos valores das parcelas em um boxplot...')

    # Marcar a hora de início
    hora_inicio = datetime.now()

    # Criar um Array Numpy com o valor da parcela
    array_valor_parcela = np.array(df_bolsa_familia['VALOR PARCELA'])

    # criar um boxplot
    plt.boxplot(array_valor_parcela, vert=False)
    plt.title('Distribuição dos valores das parcelas')

    # marcar a hora de término
    hora_fim = datetime.now()

    plt.show()

    print(f'Tempo de execução: {hora_fim - hora_inicio}')
    print('Dados visualizados com sucesso!')

except ImportError as e:
    print(f'Erro ao visualizar dados: {e}')
