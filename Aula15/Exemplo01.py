import pandas as pd
import polars as pl
import os
from datetime import datetime

os.system("cls")

try:
    ENDERECO_DADOS = r'./dados/'

    #hora de início
    hora_import = datetime.now()
    df_fevereiro = pl.read_csv(ENDERECO_DADOS + "202402_NovoBolsaFamilia.csv", separator=';', encoding='iso-8859-1')
    print (df_fevereiro)

    # hora final
    hora_impressao = datetime.now()
    
    print (f'Tempo de execução: {hora_impressao - hora_import}')

except ImportError as e:
    print("Erro ao obter dados: ", e)