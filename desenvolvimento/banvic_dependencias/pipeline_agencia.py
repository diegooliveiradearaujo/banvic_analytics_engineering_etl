import pandas as pd
from banvic_dependencias import banco_conexao as banco
import psycopg2

def agencia():
    cursor = banco.conexao()
    
    query_agencia='''select cod_agencia,nome,cidade,uf,tipo_agencia
    from erp.agencia'''

    cursor.execute(query_agencia)
    base_agencia = pd.DataFrame(cursor.fetchall())
    base_agencia.columns = ('cod_agencia','nome','cidade','uf','tipo_agencia')
    file_agencia = base_agencia.to_csv("/home/diego/airflow/data/bases_banvic/agencia.csv",index=False)
    

    ingestao_dados = '''insert into dw.agencia  (cod_agencia, nome, cidade, uf,tipo_agencia)
    select cod_agencia, nome, cidade, uf,tipo_agencia from erp.agencia
    on conflict (cod_agencia) do update
    SET cod_agencia = excluded.cod_agencia,nome=excluded.nome,
    cidade=excluded.cidade,uf=excluded.uf,tipo_agencia=excluded.tipo_agencia'''
    
    cursor.execute(ingestao_dados)