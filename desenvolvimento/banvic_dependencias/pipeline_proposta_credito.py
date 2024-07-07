import pandas as pd
from banvic_dependencias import banco_conexao as banco
import psycopg2

def credito():
	cursor = banco.conexao()
	
	query_credito='''select
	cod_proposta,
	cod_cliente,
	taxa_juros_mensal,
	cast(round(valor_proposta::numeric,2) as double precision) as valor_proposta,
	cast(round(valor_financiamento::numeric,2) as double precision) as valor_financiamento,
	cast(round(valor_entrada::numeric,2) as double precision) as valor_entrada,
	cast(round(valor_prestacao::numeric,2) as double precision) as valor_prestacao,
	quantidade_parcelas,
	status_proposta
	from erp.propostas_credito'''

	cursor.execute(query_credito)
	base_credito = pd.DataFrame(cursor.fetchall())
	
	base_credito.columns = ('cod_proposta','cod_cliente','taxa_juros_mensal','valor_proposta',
	'valor_financiamento','valor_entrada','valor_prestacao','quantidade_parcelas','status_proposta')
	file_credito = base_credito.to_csv("/home/diego/airflow/data/bases_banvic/credito.csv",index=False)
	
	
	ingestao_dados = '''insert into dw.credito (cod_proposta,cod_cliente,
	taxa_juros_mensal,valor_proposta,valor_financiamento,valor_entrada,valor_prestacao,quantidade_parcelas,status_proposta) 
	select cod_proposta,cod_cliente,taxa_juros_mensal,cast(round(valor_proposta::numeric,2) as double precision) as valor_proposta,
	cast(round(valor_financiamento::numeric,2) as double precision) as valor_financiamento,
	cast(round(valor_entrada::numeric,2) as double precision) as valor_entrada,
	cast(round(valor_prestacao::numeric,2) as double precision) as valor_prestacao,
	quantidade_parcelas,status_proposta 
	from erp.propostas_credito
	on conflict (cod_proposta) do update
	set cod_proposta=excluded.cod_proposta,cod_cliente=excluded.cod_cliente,taxa_juros_mensal=excluded.taxa_juros_mensal,
	valor_proposta=excluded.valor_proposta,valor_financiamento=excluded.valor_financiamento,valor_entrada=excluded.valor_entrada,
	valor_prestacao=excluded.valor_prestacao,quantidade_parcelas=excluded.quantidade_parcelas,status_proposta=excluded.status_proposta'''
	cursor.execute(ingestao_dados)