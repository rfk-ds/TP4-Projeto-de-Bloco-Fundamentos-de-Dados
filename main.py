import sqlite3
import csv
import json

def conectar():
    return sqlite3.connect("banco_de_dados_tp.db")

def criar_tabelas(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Cargos (
        id_cargo INTEGER PRIMARY KEY,
        nome_cargo TEXT NOT NULL,
        descricao_cargo TEXT NOT NULL
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Departamentos (
        id_departamento INTEGER PRIMARY KEY,
        nome_departamento TEXT NOT NULL,
        email_departamento TEXT NOT NULL
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Funcionarios (
        id_funcionario INTEGER PRIMARY KEY,
        nome TEXT NOT NULL,
        data_admissao TEXT NOT NULL,
        id_cargo INTEGER,
        id_departamento INTEGER,
        salario_base REAL,
        FOREIGN KEY (id_cargo) REFERENCES Cargos (id_cargo),
        FOREIGN KEY (id_departamento) REFERENCES Departamentos (id_departamento)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS HistoricoSalarios (
        id_historico INTEGER PRIMARY KEY,
        id_funcionario INTEGER,
        mes_ano TEXT NOT NULL,
        salario REAL NOT NULL,
        FOREIGN KEY (id_funcionario) REFERENCES Funcionarios (id_funcionario)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Dependentes (
        id_dependente INTEGER PRIMARY KEY,
        nome TEXT NOT NULL,
        data_nascimento TEXT NOT NULL,
        sexo TEXT NOT NULL,
        id_funcionario INTEGER,
        FOREIGN KEY (id_funcionario) REFERENCES Funcionarios (id_funcionario)
    );
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Projetos (
        id_projeto INTEGER PRIMARY KEY,
        nome_projeto TEXT NOT NULL,
        descricao TEXT NOT NULL,
        data_inicio TEXT NOT NULL,
        data_conclusao TEXT,
        id_funcionario_responsavel INTEGER,
        custo REAL NOT NULL,
        status TEXT CHECK(status IN ('Em Planejamento', 'Em Execução', 'Concluído', 'Cancelado')),
        FOREIGN KEY (id_funcionario_responsavel) REFERENCES Funcionarios (id_funcionario)
    );
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Recursos (
        id_recurso INTEGER PRIMARY KEY,
        id_projeto INTEGER,
        descricao_recurso TEXT NOT NULL,
        tipo_recurso TEXT CHECK(tipo_recurso IN ('financeiro', 'material', 'humano')),
        quantidade_utilizada REAL NOT NULL,
        data_utilizacao TEXT NOT NULL,
        FOREIGN KEY (id_projeto) REFERENCES Projetos (id_projeto)
    );
    """)
    
def limpar_tabelas(cursor):
    cursor.execute("DELETE FROM Dependentes;")
    cursor.execute("DELETE FROM HistoricoSalarios;")
    cursor.execute("DELETE FROM Funcionarios;")
    cursor.execute("DELETE FROM Departamentos;")
    cursor.execute("DELETE FROM Cargos;")
    cursor.execute("DELETE FROM Recursos;")
    cursor.execute("DELETE FROM Projetos;")


def inserir_dados_csv(cursor):
    cargos = ler_csv('cargos.csv')
    departamentos = ler_csv('departamentos.csv')
    funcionarios = ler_csv('funcionarios.csv')
    historico_salarios = ler_csv('historico_salarios.csv')
    dependentes = ler_csv('dependentes.csv')
    recursos = ler_csv('recursos.csv')
    projetos = ler_csv('projetos.csv')

    cursor.executemany("""
        INSERT INTO Cargos (nome_cargo, descricao_cargo)
        VALUES (?, ?)
    """, cargos)

    cursor.executemany("""
        INSERT INTO Departamentos (nome_departamento, email_departamento)
        VALUES (?, ?)
    """, departamentos)

    cursor.executemany("""
        INSERT INTO Funcionarios (nome, data_admissao, id_cargo, id_departamento, salario_base)
        VALUES (?, ?, ?, ?, ?)
    """, funcionarios)

    cursor.executemany("""
        INSERT INTO HistoricoSalarios (id_funcionario, mes_ano, salario)
        VALUES (?, ?, ?)
    """, historico_salarios)

    cursor.executemany("""
        INSERT INTO Dependentes (nome, data_nascimento, sexo, id_funcionario)
        VALUES (?, ?, ?, ?)
    """, dependentes)
    
    cursor.executemany("""
        INSERT INTO Recursos (id_projeto, descricao_recurso, tipo_recurso, quantidade_utilizada, data_utilizacao)
        VALUES (?, ?, ?, ?, ?)
    """, recursos)
    
    cursor.executemany("""
        INSERT INTO Projetos (nome_projeto, descricao, data_inicio, data_conclusao, id_funcionario_responsavel, custo, status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, projetos)

def criar_csvs():
    print("Criando arquivos CSV...")

    cargos = [
        ['Desenvolvedor', 'Desenvolve e mantém sistemas'],
        ['Gerente', 'Gerencia equipes e projetos'],
        ['Analista', 'Analisa processos e propõe melhorias'],
        ['Designer', 'Cria designs e interfaces'],
        ['Suporte Técnico', 'Resolve problemas de TI e suporte'],
        ['Estagiário', 'Auxilia nas atividades diárias']
    ]
    
    departamentos = [
        ['TI', 'ti@empresa.com'],
        ['Recursos Humanos', 'rh@empresa.com'],
        ['Marketing', 'marketing@empresa.com'],
        ['Vendas', 'vendas@empresa.com'],
        ['Financeiro', 'financeiro@empresa.com']
    ]
    
    funcionarios = [
        ['João Silva', '2023-01-15', 1, 1, 5000.0],
        ['Maria Oliveira', '2023-02-20', 2, 2, 6000.0],
        ['Pedro Souza', '2022-11-05', 3, 3, 6300.0],
        ['Ana Costa', '2021-07-13', 4, 4, 7000.0],
        ['Luiz Pereira', '2020-08-21', 5, 5, 3000.0],
        ['Carlos Nunes', '2023-03-10', 6, 6, 1500.0],
        ['Alice Oliveira', '2023-04-25', 5, 7, 6500.0],
        ['Bruno Santos', '2023-05-30', 3, 8, 6700.0],
        ['Carla Dias', '2023-06-05', 1, 9, 5200.0],
        ['Nicolas lima', '2023-07-10', 2, 10, 6000.0]  
    ]
    
    historico_salarios = [
        [1, '2023-09', 5000.0], [1, '2023-08', 4800.0], [1, '2023-07', 4600.0],
        [2, '2023-09', 6000.0], [2, '2023-08', 6000.0], [2, '2023-07', 5800.0],
        [3, '2023-09', 6300.0], [3, '2023-08', 6300.0], [3, '2023-07', 6300.0],
        [4, '2023-09', 8000.0], [4, '2023-08', 7900.0], [4, '2023-07', 7800.0],
        [5, '2023-09', 3000.0], [5, '2023-08', 3000.0], [5, '2023-07', 3000.0],
        [6, '2023-09', 1500.0], [6, '2023-08', 1500.0], [6, '2023-07', 1500.0],
        [7, '2023-09', 6500.0], [7, '2023-08', 6500.0], [7, '2023-07', 6500.0],
        [8, '2023-09', 6700.0], [8, '2023-08', 6700.0], [8, '2023-07', 6700.0],
        [9, '2023-09', 5200.0], [9, '2023-08', 5200.0], [9, '2023-07', 5200.0],
        [10, '2023-09', 6000.0], [10, '2023-08', 6000.0], [10, '2023-07', 6000.0]
    ]
    
    dependentes = [
        ['Filho João', '2015-05-10', 'M', 1],
        ['Filha Maria', '2017-09-23', 'F', 1],
        ['Filho Pedro', '2016-12-01', 'M', 2],
        ['Filha Ana', '2019-03-14', 'F', 2],
        ['Filha Sofia', '2018-11-12', 'F', 3],
        ['Filho Miguel', '2019-06-15', 'M', 4],
        ['Filha Clara', '2020-02-10', 'F', 3],
        ['Filha Gabriela', '2022-09-23', 'F', 4],
        ['Filho Lucas', '2021-11-15', 'M', 5],  
        ['Filha Laura', '2022-05-01', 'F', 6]
    ]
    
    recursos = [
        [1, 'Recursos financeiros para o projeto', 'financeiro', 2000.0, '2023-09-15'],
        [2, 'Recursos materiais para o projeto', 'material', 100.0, '2023-09-15'],
        [3, 'Recursos humanos para o projeto', 'humano', 10.0, '2023-09-15'],
        [4, 'Recursos financeiros para o projeto', 'financeiro', 1500.0, '2023-09-15'],
        [5, 'Recursos materiais para o projeto', 'material', 50.0, '2023-09-15'],
        [6, 'Recursos humanos para o projeto', 'humano', 5.0, '2023-09-15'],
        [7, 'Recursos financeiros para o projeto', 'financeiro', 1000.0, '2023-09-15'],
        [8, 'Recursos materiais para o projeto', 'material', 75.0, '2023-09-15'],
        [9, 'Recursos humanos para o projeto', 'humano', 8.0, '2023-09-15'],
        [10, 'Recursos financeiros para o projeto', 'financeiro', 1800.0, '2023-09-15'],
        [11, 'Recursos materiais para o projeto', 'material', 90.0, '2023-09-15'],
        [12, 'Recursos humanos para o projeto', 'humano', 12.0, '2023-09-15']
    ]
    
    projetos = [
        ['Projeto 1', 'Projeto de desenvolvimento de software', '2023-09-15', '2023-12-15', 1, 5000.0, 'Em Planejamento'],
        ['Projeto 2', 'Projeto de marketing digital', '2023-09-15', '2023-12-15', 2, 6000.0, 'Em Execução'],
        ['Projeto 3', 'Projeto de vendas', '2023-09-15', '2023-12-15', 3, 6300.0, 'Concluído'],
        ['Projeto 4', 'Projeto de finanças', '2023-09-15', '2023-12-15', 4, 7000.0, 'Em Planejamento'],
        ['Projeto 5', 'Projeto de recursos humanos', '2023-09-15', '2023-12-15', 5, 3000.0, 'Em Planejamento'],
        ['Projeto 6', 'Projeto de vendas', '2023-09-15', '2023-12-15', 6, 1500.0, 'Em Planejamento'],
        ['Projeto 7', 'Projeto de marketing digital', '2023-09-15', '2023-12-15', 7, 6500.0, 'Cancelado'],
        ['Projeto 8', 'Projeto de desenvolvimento de software', '2023-09-15', '2023-12-15', 8, 6700.0, 'Concluído'],
        ['Projeto 9', 'Projeto de marketing digital', '2023-09-15', '2023-12-15', 9, 5200.0, 'Em Planejamento'],
        ['Projeto 10', 'Projeto de vendas', '2023-09-15', '2023-12-15', 10, 6000.0, 'Em Execução'],
        ['Projeto 11', 'Projeto de finanças', '2023-09-15', '2023-12-15', 1, 5000.0, 'Em Planejamento'],
        ['Projeto 12', 'Projeto de recursos humanos', '2023-09-15', '2023-12-15', 2, 6000, 'Concluído'],
        ['Projeto 13', 'Projeto de vendas', '2023-09-15', '2023-12-15', 3, 6300.0, 'Concluído'],
        ['Projeto 14', 'Projeto de finanças', '2023-09-15', '2023-12-15', 4, 7000.0, 'Em Planejamento'],
        ['Projeto 15', 'Projeto de recursos humanos', '2023-09-15', '2023-12-15', 5, 3000.0, 'Em Planejamento'],
        ['Projeto 16', 'Projeto de vendas', '2023-09-15', '2023-12-15', 6, 1500.0, 'Em Planejamento'],
        ['Projeto 17', 'Projeto de marketing digital', '2023-09-15', '2023-12-15', 7, 6500.0, 'Cancelado'],
        ['Projeto 18', 'Projeto de desenvolvimento de software', '2023-09-15', '2023-12-15', 8, 6700.0, 'Concluído'],
        ['Projeto 19', 'Projeto de marketing digital', '2023-09-15', '2023-12-15', 9, 5200.0, 'Em Planejamento'],
        ['Projeto 20', 'Projeto de vendas', '2023-09-15', '2023-12-15', 10, 6000.0, 'Em Execução']
    ]

    criar_csv('cargos.csv', ['nome_cargo', 'descricao_cargo'], cargos)
    criar_csv('departamentos.csv', ['nome_departamento', 'email_departamento'], departamentos)
    criar_csv('funcionarios.csv', ['nome', 'data_admissao', 'id_cargo', 'id_departamento', 'salario_base'], funcionarios)
    criar_csv('historico_salarios.csv', ['id_funcionario', 'mes_ano', 'salario'], historico_salarios)
    criar_csv('dependentes.csv', ['nome', 'data_nascimento', 'sexo', 'id_funcionario'], dependentes)
    criar_csv('recursos.csv', ['id_projeto', 'descricao_recurso', 'tipo_recurso', 'quantidade_utilizada', 'data_utilizacao'], recursos)
    criar_csv('projetos.csv', ['nome_projeto', 'descricao', 'data_inicio', 'data_conclusao', 'id_funcionario_responsavel', 'custo', 'status'], projetos)

def criar_csv(nome_arquivo, cabecalho, dados):
    print(f"Criando arquivo {nome_arquivo}...")
    with open(nome_arquivo, mode='w', encoding='utf-8', newline='') as arquivo:
        escritor = csv.writer(arquivo)
        escritor.writerow(cabecalho)
        escritor.writerows(dados)
    print(f"{nome_arquivo} criado com sucesso!\n")


def ler_csv(nome_arquivo):
    with open(nome_arquivo, mode='r', encoding='utf-8') as arquivo:
        leitor = csv.reader(arquivo)
        next(leitor)
        return [linha for linha in leitor if linha]

def salvar_json(nome_arquivo, dados):
    with open(nome_arquivo, 'w', encoding='utf-8') as arquivo:
        json.dump(dados, arquivo, ensure_ascii=False, indent=4)
    print(f"{nome_arquivo} criado com sucesso!")

# Trazer a média dos salários (atual) dos funcionários responsáveis por projetos concluídos, agrupados por departamento"
def consulta_1(cursor):
    cursor.execute("""
    SELECT d.nome_departamento, AVG(f.salario_base)
    FROM Funcionarios f
    JOIN Projetos p ON f.id_funcionario = p.id_funcionario_responsavel
    JOIN Departamentos d ON f.id_departamento = d.id_departamento
    WHERE p.status = 'Concluído'
    GROUP BY d.nome_departamento
    """)
    resultado = cursor.fetchall()
    print("Departamento | Média dos salários")
    for linha in resultado:
        print(linha)
    
    dados = [{"departamento": linha[0], "media_salario": linha[1]} for linha in resultado]
    return dados

# Identificar os três recursos materiais mais usados nos projetos, listando a descrição do recurso e a quantidade total usada.
def consulta_2(cursor):
    cursor.execute("""
    SELECT r.descricao_recurso, SUM(r.quantidade_utilizada) as quantidade_total
    FROM Recursos r
    WHERE r.tipo_recurso = 'material'
    GROUP BY r.descricao_recurso
    ORDER BY quantidade_total DESC
    LIMIT 3
    """)
    resultado = cursor.fetchall()
    print("Descrição do recurso | Quantidade total usada")
    for linha in resultado:
        print(linha)
    
        
# Calcular o custo total dos projetos por departamento, considerando apenas os projetos 'Concluídos'.
def consulta_3(cursor):
    cursor.execute("""
    SELECT d.nome_departamento, SUM(p.custo) as custo_total
    FROM Projetos p
    JOIN Funcionarios f ON p.id_funcionario_responsavel = f.id_funcionario
    JOIN Departamentos d ON f.id_departamento = d.id_departamento
    WHERE p.status = 'Concluído'
    GROUP BY d.nome_departamento
    """)
    resultado = cursor.fetchall()
    print("Departamento | Custo total dos projetos")
    for linha in resultado:
        print(linha)
    
    dados = [{"departamento": linha[0], "custo_total": linha[1]} for linha in resultado]
    return dados
        
# Listar todos os projetos com seus respectivos nomes, custo, data de início, data de conclusão e o nome do funcionário responsável, que estejam 'Em Execução'.
def consulta_4(cursor):
    cursor.execute("""
    SELECT p.nome_projeto, p.custo, p.data_inicio, p.data_conclusao, f.nome
    FROM Projetos p
    JOIN Funcionarios f ON p.id_funcionario_responsavel = f.id_funcionario
    WHERE p.status = 'Em Execução'
    """)
    resultado = cursor.fetchall()
    print("Nome do projeto | Custo | Data de início | Data de conclusão | Nome do funcionário responsável")
    for linha in resultado:
        print(linha)

    dados = [{"nome_projeto": linha[0], "custo": linha[1], "data_inicio": linha[2],
              "data_conclusao": linha[3], "responsavel": linha[4]} for linha in resultado]
    return dados

# Identificar o projeto com o maior número de dependentes envolvidos, considerando que os dependentes são associados aos funcionários que estão gerenciando os projetos.
def consulta_5(cursor):
    cursor.execute("""
    SELECT p.nome_projeto, COUNT(d.id_dependente) as total_dependentes
    FROM Projetos p
    JOIN Funcionarios f ON p.id_funcionario_responsavel = f.id_funcionario
    JOIN Dependentes d ON f.id_funcionario = d.id_funcionario
    GROUP BY p.nome_projeto
    ORDER BY total_dependentes DESC
    LIMIT 1
    """)
    resultado = cursor.fetchall()
    print("Nome do projeto | Total de dependentes")
    for linha in resultado:
        print(linha)

def main():
    conn = conectar()
    cursor = conn.cursor()
    
    criar_tabelas(cursor)
    limpar_tabelas(cursor)
    criar_csvs()
    inserir_dados_csv(cursor)
    
    print("------------------- CONSULTAS -------------------")
    print("\n")
    
    print("1. Trazer a média dos salários (atual) dos funcionários responsáveis por projetos concluídos, agrupados por departamento")
    consulta_1(cursor)
    print("\n")
    
    print("2. Identificar os três recursos materiais mais usados nos projetos, listando a descrição do recurso e a quantidade total usada.")
    consulta_2(cursor)
    print("\n")
    
    print("3. Calcular o custo total dos projetos por departamento, considerando apenas os projetos 'Concluídos'.")
    consulta_3(cursor)
    print("\n")
    
    print("4. Listar todos os projetos com seus respectivos nomes, custo, data de início, data de conclusão e o nome do funcionário responsável, que estejam 'Em Execução'.")
    consulta_4(cursor)
    print("\n")
    
    print("5. Identificar o projeto com o maior número de dependentes envolvidos, considerando que os dependentes são associados aos funcionários que estão gerenciando os projetos.")
    consulta_5(cursor)
    print("\n")
    
    print("------------------- RESULTADOS (json) -------------------\n")
    salvar_json('consulta_1_media_salarios.json', consulta_1(cursor))
    salvar_json('consulta_3_custo_projetos.json', consulta_3(cursor))
    salvar_json('consulta_4_projetos_execucao.json', consulta_4(cursor))

    conn.commit()
    conn.close()

main()