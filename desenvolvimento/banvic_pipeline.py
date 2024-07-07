from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from banvic_dependencias import pipeline_agencia as agencia
from banvic_dependencias import pipeline_clientes as clientes
from banvic_dependencias import pipeline_contas as contas
from banvic_dependencias import pipeline_proposta_credito as credito
from banvic_dependencias import pipeline_transacoes as transacoes
from banvic_dependencias import create_schema_tabelas as criacao


#from dotenv import load_dotenv 
#import os

with DAG("pipeline_banvic",start_date=datetime(2024,2,18),
         schedule_interval='* */5 * * *') as dag:#,template_searchpath='/home/diego/airflow/sql') as dag:
   
    criacao_schema_tabelas = PythonOperator(
        task_id = 'criacao_schema_tabelas',
        python_callable = criacao.criacao
        #postgres_conn_id = 'postgres_airflow',
        #sql = 'insere_dados_db.sql' 
        )

    pipeline_agencia = PythonOperator(
        task_id = 'pipeline_agencia',
        python_callable = agencia.agencia
        #postgres_conn_id = 'postgres_airflow',
        #sql = 'insere_dados_db.sql' 
        )

    pipeline_clientes = PythonOperator(
        task_id = 'pipeline_clientes',
        python_callable = clientes.clientes
        #postgres_conn_id = 'postgres_airflow',
        #sql = 'insere_dados_db.sql' 
        )

    pipeline_contas = PythonOperator(
        task_id = 'pipeline_contas',
        python_callable = contas.contas
        #postgres_conn_id = 'postgres_airflow',
        #sql = 'insere_dados_db.sql' 
        )

    pipeline_credito = PythonOperator(
        task_id = 'pipeline_credito',
        python_callable = credito.credito
        #postgres_conn_id = 'postgres_airflow',
        #sql = 'insere_dados_db.sql' 
        )

    pipeline_transacoes = PythonOperator(
        task_id = 'pipeline_transacoes',
        python_callable = transacoes.transacoes
        #postgres_conn_id = 'postgres_airflow',
        #sql = 'insere_dados_db.sql' 
        )

criacao_schema_tabelas>>[pipeline_agencia,pipeline_clientes,pipeline_contas,pipeline_credito,pipeline_transacoes]