import psycopg2
from banvic_dependencias import banco_conexao as banco

def criacao(): 
    cursor = banco.conexao()
    
    query_criacoes='''
    create schema if not exists dw;
    create table if not exists dw.agencia
    (   cod_agencia int primary key not null,
        nome varchar,
        cidade varchar,
        uf varchar,
        tipo_agencia varchar
    );
    
    create table if not exists dw.clientes(
        cod_cliente int primary key not null,
        nome varchar,
        data_inclusao date
    );
    
    create table if not exists dw.contas(
        cod_cliente int primary key not null,
        cod_agencia int,
        saldo_total double precision,
        saldo_disponivel double precision
    );
    
    create table if not exists dw.credito(
        cod_proposta int primary key not null,
        cod_cliente int,
        taxa_juros_mensal double precision,
        valor_proposta double precision,
        valor_financiamento double precision,
        valor_entrada double precision,
        valor_prestacao double precision,
        quantidade_parcelas int,
        status_proposta varchar
    );
    
    create table if not exists dw.transacoes(
    cod_transacao int primary key not null,
    cod_cliente int,
    data_transacao date,
    nome_transacao varchar,
    valor_transacao double precision 
    )'''
        
    cursor.execute(query_criacoes)