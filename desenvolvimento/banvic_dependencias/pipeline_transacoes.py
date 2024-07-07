import pandas as pd
from banvic_dependencias import banco_conexao as banco
import psycopg2

def transacoes():
    cursor = banco.conexao()
    
    query_transacoes='''select
    cod_transacao,
    num_conta as cod_cliente,
    cast(data_transacao as date) as data_transacao ,
    nome_transacao,
    valor_transacao 
    from erp.transacoes'''

    cursor.execute(query_transacoes)
    base_transacoes = pd.DataFrame(cursor.fetchall())
    
    base_transacoes.columns = ('cod_transacao', 'cod_cliente', 'data_transacao', 'nome_transacao', 'valor_transacao')
    file_transacoes = base_transacoes.to_csv("/home/diego/airflow/data/bases_banvic/transacoes.csv",index=False)
    
    ingestao_dados = '''insert into dw.transacoes(cod_transacao,cod_cliente,data_transacao,nome_transacao,valor_transacao)
    select cod_transacao,num_conta as cod_cliente,cast(data_transacao as date) as data_transacao,nome_transacao,valor_transacao 
    from erp.transacoes
    on conflict (cod_transacao) do update
	set cod_transacao=excluded.cod_transacao,cod_cliente=excluded.cod_cliente,
    data_transacao=excluded.data_transacao,nome_transacao=excluded.nome_transacao,valor_transacao=excluded.valor_transacao'''

    cursor.execute(ingestao_dados)