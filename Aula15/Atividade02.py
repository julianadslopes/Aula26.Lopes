import pandas as pd
import polars as pl
import gc

from datetime import datetime

ENDERECO_DADOS = r'./dados/'

try:

    #hora de início
    inicio = datetime.now()
    lista_arquivos = ["202404_NovoBolsaFamilia.csv", "202405_NovoBolsaFamilia.csv"]

    for arquivo in lista_arquivos:
        print (f'Processando arquivo {arquivo}')
    
        df = pl.read_csv(ENDERECO_DADOS + arquivo, separator=';', encoding= 'iso-8859-1')
        if "df_bolsa_familia" in locals():
            df_bolsa_familia = pl.concat ([df_bolsa_familia,df])
        else:
            df_bolsa_familia = df

        # Limpar df da memória
        del df

        print (df_bolsa_familia)            

        print(f'Arquivo {arquivo} processados com sucesso!')

    # Converte a coluna "VALOR PARCELA" para o tipo float
    df_bolsa_familia = df_bolsa_familia.with_columns(pl.col('VALOR PARCELA').str.replace(',','.').cast(pl.Float64))

    print('\nDados dos DataFrames concatenados com sucesso!')
    print('Iniciando a gravação do arquivo Parquet...')

    df_bolsa_familia.write_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')

    # Deletar df_bolsa_familia da memoria
    del df_bolsa_familia
   
    # coletar resíduos da memória
    gc.collect()

    # hora final
    final = datetime.now()
    
    print('Gravação do arquivo Parquet realizada com sucesso!')
    print (f' Tempo de execução: {final - inicio}')
    
except ImportError as e:
    print("Erro ao obter dados: ", e)