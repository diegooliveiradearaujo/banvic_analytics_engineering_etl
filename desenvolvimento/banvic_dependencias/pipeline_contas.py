import pandas as pd
from banvic_dependencias import banco_conexao as banco
import psycopg2

def contas():
    cursor = banco.conexao()
    
    query_contas='''select
    cod_cliente,
    cod_agencia,
    cast(round(saldo_total::numeric,2) as double precision) as saldo_total,
    cast(round(saldo_disponivel::numeric,2) as double precision) as saldo_disponivel
    from erp.contas'''

    cursor.execute(query_contas)
    base_contas = pd.DataFrame(cursor.fetchall())
    
    base_contas.columns = ('cod_cliente','cod_agencia','saldo_total','saldo_disponivel')
    file_contas = base_contas.to_csv("/home/diego/airflow/data/bases_banvic/contas.csv",index=False)


    ingestao_dados = '''insert into dw.contas (cod_cliente,cod_agencia,saldo_total,saldo_disponivel)
    select cod_cliente,cod_agencia, cast(round(saldo_total::numeric,2) as double precision) as saldo_total,
    cast(round(saldo_disponivel::numeric,2) as double precision) as saldo_disponivel
    from erp.contas
    on conflict (cod_cliente) do update
    set cod_cliente = excluded.cod_cliente,cod_agencia=excluded.cod_agencia,saldo_total=excluded.saldo_total,
    saldo_disponivel=excluded.saldo_disponivel'''
    
    cursor.execute(ingestao_dados)