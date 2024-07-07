import pandas as pd
from banvic_dependencias import banco_conexao as banco
import psycopg2

def clientes():
    cursor = banco.conexao()
    
    query_agencia='''select cod_cliente,primeiro_nome||' '||ultimo_nome as nome,data_inclusao
    from erp.clientes'''

    cursor.execute(query_agencia)
    base_clientes = pd.DataFrame(cursor.fetchall())
    base_clientes.columns = ('cod_cliente','nome','data_inclusao')
    file_clientes = base_clientes.to_csv("/home/diego/airflow/data/bases_banvic/clientes.csv",index=False)
    

    ingestao_dados = '''insert into dw.clientes (cod_cliente,nome,data_inclusao)
    select cod_cliente,primeiro_nome||' '||ultimo_nome as nome,data_inclusao
    from erp.clientes
    on conflict (cod_cliente) do update
    set cod_cliente = excluded.cod_cliente,nome=excluded.nome,
    data_inclusao=excluded.data_inclusao'''
    cursor.execute(ingestao_dados)