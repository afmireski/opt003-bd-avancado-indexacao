from faker import Faker
import random
from datetime import datetime, timedelta
import json
from utils.db_connection import execute_query, execute_many, DatabaseConnection
import psycopg2.extras

# Registrar o adaptador para arrays
psycopg2.extras.register_default_jsonb()


class BibliotecaDataGenerator:
    def __init__(self, locales=["pt_BR"], seed=42):
        """
        Inicializa o gerador de dados com Faker.

        Args:
            locales: Lista de localidades para Faker usar
            seed: Semente aleatória para garantir reprodutibilidade
        """
        self.fake = Faker(locales)
        Faker.seed(seed)
        random.seed(seed)
        self.generos_possiveis = [
            "Romance",
            "Ficção Científica",
            "Fantasia",
            "Terror",
            "Drama",
            "Aventura",
            "Biografia",
            "História",
            "Poesia",
            "Suspense",
            "Infantil",
            "Autoajuda",
            "Acadêmico",
            "Técnico",
            "Filosofia",
            "Religião",
            "Política",
            "Economia",
            "Culinária",
            "Esporte",
        ]

    def create_schema(self):
        """
        Cria as tabelas no banco de dados se elas não existirem.
        """
        schema_sql = """
        -- Tabela de usuários da biblioteca
        CREATE TABLE IF NOT EXISTS usuarios (
            id SERIAL PRIMARY KEY,
            nome TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            cpf VARCHAR(14) NOT NULL UNIQUE,
            data_nascimento DATE NOT NULL
        );

        -- Tabela de livros
        CREATE TABLE IF NOT EXISTS livros (
            id SERIAL PRIMARY KEY,
            isbn VARCHAR(20) NOT NULL UNIQUE,
            titulo TEXT NOT NULL,
            autor TEXT NOT NULL,
            genero TEXT NOT NULL,
            citacoes TEXT[],
            metadados JSONB,
            dimensao POLYGON,
            publicado_em DATE,
            disponivel BOOLEAN DEFAULT TRUE
        );

        -- Tabela de empréstimos
        CREATE TABLE IF NOT EXISTS emprestimos (
            id SERIAL PRIMARY KEY,
            livro_id INTEGER NOT NULL REFERENCES livros(id),
            usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
            data_emprestimo DATE NOT NULL,
            data_devolucao DATE,
            status VARCHAR(20) NOT NULL
        );
        """
        execute_query(schema_sql)
        print("Esquema criado com sucesso.")

    def gerar_cpf(self):
        """Gera um CPF formatado"""
        return f"{random.randint(100, 999)}.{random.randint(100, 999)}.{random.randint(100, 999)}-{random.randint(10, 99)}"

    def gerar_isbn(self):
        """Gera um ISBN-13 formatado"""
        return f"978-{random.randint(0, 9)}-{random.randint(100000, 999999)}-{random.randint(10, 99)}-{random.randint(0, 9)}"

    def gerar_poligono_dimensao(self):
        """Gera um polígono para representar dimensões do livro (L,A,P)"""
        # Gera 4 pontos para formar um polígono retangular
        # Formato aproximado para POLYGON((x1 y1, x2 y2, x3 y3, x4 y4, x1 y1))
        largura = random.uniform(10, 30)  # cm
        altura = random.uniform(15, 40)  # cm
        profundidade = random.uniform(1, 5)  # cm

        # Criando os pontos do polígono
        points = [
            (0, 0),  # ponto inicial
            (largura, 0),  # largura
            (largura, altura),  # diagonal
            (0, altura),  # altura
            (0, 0),  # fechando o polígono
        ]

        # Convertendo para string de polígono
        polygon_str = f"((0,0),({largura},0),({largura},{altura}),(0, {altura}), (0,0))"
        return polygon_str

    def gerar_usuario(self):
        """Gera dados para um usuário"""
        nome = self.fake.name()
        email = self.fake.email()
        cpf = self.gerar_cpf()
        data_nascimento = self.fake.date_of_birth(minimum_age=5, maximum_age=90)

        return {
            "nome": nome,
            "email": email,
            "cpf": cpf,
            "data_nascimento": data_nascimento,
        }

    def gerar_livro(self):
        """Gera dados para um livro"""
        titulo = self.fake.catch_phrase()
        autor = self.fake.name()
        isbn = self.gerar_isbn()

        # Selecionar apenas um gênero ao invés de múltiplos
        genero = random.choice(self.generos_possiveis)

        # Gerar entre 0 e 5 citações
        num_citacoes = random.randint(0, 5)
        citacoes = (
            [self.fake.paragraph(nb_sentences=1) for _ in range(num_citacoes)]
            if num_citacoes > 0
            else None
        )

        # Metadados em formato JSON
        metadados = {
            "num_paginas": random.randint(50, 1000),
            "editora": self.fake.company(),
            "edicao": random.randint(1, 10),
            "idioma": random.choice(["Português", "Inglês", "Espanhol", "Francês"]),
            "peso": round(random.uniform(0.1, 3.0), 2),  # peso em kg
        }

        dimensao = self.gerar_poligono_dimensao()
        publicado_em = self.fake.date_between(start_date="-100y", end_date="today")
        disponivel = random.choice(
            [True, True, True, False]
        )  # 75% de chance de estar disponível

        return {
            "isbn": isbn,
            "titulo": titulo,
            "autor": autor,
            "genero": genero,  # Agora é um único valor, não um array
            "citacoes": citacoes,
            "metadados": json.dumps(metadados),
            "dimensao": dimensao,
            "publicado_em": publicado_em,
            "disponivel": disponivel,
        }

    def gerar_emprestimo(self, livro_id, usuario_id):
        """Gera dados para um empréstimo"""
        data_emprestimo = self.fake.date_between(start_date="-3y", end_date="today")

        # 80% de chance de ter uma data de devolução
        tem_devolucao = random.random() < 0.8

        if tem_devolucao:
            dias_ate_devolucao = random.randint(1, 60)
            data_devolucao = data_emprestimo + timedelta(days=dias_ate_devolucao)
            status = random.choice(["Devolvido", "Atrasado"])
        else:
            data_devolucao = None
            status = random.choice(["Em Andamento", "Perdido", "Danificado"])

        return {
            "livro_id": livro_id,
            "usuario_id": usuario_id,
            "data_emprestimo": data_emprestimo,
            "data_devolucao": data_devolucao,
            "status": status,
        }

    def inserir_usuarios(self, quantidade=1000, batch_size=1000):
        """
        Insere usuários em lote no banco de dados.

        Args:
            quantidade: Número de usuários a serem gerados
            batch_size: Tamanho do lote para inserção
        """
        print(f"Gerando {quantidade} usuários...")

        # SQL para inserção
        sql = """
        INSERT INTO usuarios (nome, email, cpf, data_nascimento)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (email) DO NOTHING
        """

        execute_many(
            sql,
            [
                (
                    "Vitória Albuquerque",
                    "gustavo-henrique24@example.org",
                    "532.710.165-59",
                    "2010-11-20",
                )
            ],
        )

        total_inseridos = 1

        for i in range(0, quantidade, batch_size):
            batch_size_atual = min(batch_size, quantidade - i)
            batch_usuarios = [self.gerar_usuario() for _ in range(batch_size_atual)]

            # Preparar os parâmetros para inserção em lote
            params = [
                (
                    usuario["nome"],
                    usuario["email"],
                    usuario["cpf"],
                    usuario["data_nascimento"],
                )
                for usuario in batch_usuarios
            ]

            # Executar inserção em lote
            rows_affected = execute_many(sql, params)
            total_inseridos += rows_affected

            print(f"Inseridos {total_inseridos} usuários de {quantidade}...")

        return total_inseridos

    def inserir_livros(self, quantidade=2000, batch_size=1000):
        """
        Insere livros em lote no banco de dados.

        Args:
            quantidade: Número de livros a serem gerados
            batch_size: Tamanho do lote para inserção
        """
        print(f"Gerando {quantidade} livros...")

        # SQL para inserção - modificado para usar genero em vez de generos
        sql = """
        INSERT INTO livros (isbn, titulo, autor, genero, citacoes, metadados, dimensao, publicado_em, disponivel)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::polygon, %s, %s)
        ON CONFLICT (isbn) DO NOTHING
        """

        total_inseridos = 0

        for i in range(0, quantidade, batch_size):
            batch_size_atual = min(batch_size, quantidade - i)
            batch_livros = [self.gerar_livro() for _ in range(batch_size_atual)]

            # Preparar os parâmetros para inserção em lote
            params = [
                (
                    livro["isbn"],
                    livro["titulo"],
                    livro["autor"],
                    livro["genero"],  # Agora é um único valor, não um array
                    livro["citacoes"],
                    livro["metadados"],
                    livro["dimensao"],
                    livro["publicado_em"],
                    livro["disponivel"],
                )
                for livro in batch_livros
            ]

            # Executar inserção em lote
            rows_affected = execute_many(sql, params)
            total_inseridos += rows_affected

            print(f"Inseridos {total_inseridos} livros de {quantidade}...")

        return total_inseridos

    def inserir_emprestimos(self, quantidade=50000, batch_size=5000):
        """
        Insere empréstimos em lote no banco de dados.

        Args:
            quantidade: Número de empréstimos a serem gerados
            batch_size: Tamanho do lote para inserção
        """
        print(f"Gerando {quantidade} empréstimos...")

        # Primeiro, obtém os IDs de usuários e livros disponíveis
        usuarios_ids = execute_query("SELECT id FROM usuarios").id.tolist()
        livros_ids = execute_query("SELECT id FROM livros").id.tolist()

        if not usuarios_ids or not livros_ids:
            print(
                "Não há usuários ou livros suficientes no banco para gerar empréstimos."
            )
            return 0

        # SQL para inserção
        sql = """
        INSERT INTO emprestimos (livro_id, usuario_id, data_emprestimo, data_devolucao, status)
        VALUES (%s, %s, %s, %s, %s)
        """

        total_inseridos = 0

        for i in range(0, quantidade, batch_size):
            batch_size_atual = min(batch_size, quantidade - i)
            batch_params = []

            for _ in range(batch_size_atual):
                livro_id = random.choice(livros_ids)
                usuario_id = random.choice(usuarios_ids)
                emprestimo = self.gerar_emprestimo(livro_id, usuario_id)

                batch_params.append(
                    (
                        emprestimo["livro_id"],
                        emprestimo["usuario_id"],
                        emprestimo["data_emprestimo"],
                        emprestimo["data_devolucao"],
                        emprestimo["status"],
                    )
                )

            # Executar inserção em lote
            rows_affected = execute_many(sql, batch_params)
            total_inseridos += rows_affected

            print(f"Inseridos {total_inseridos} empréstimos de {quantidade}...")

        return total_inseridos

    def popular_banco(
        self, num_usuarios=2000, num_livros=4000, num_emprestimos=6000, clear=False
    ):
        """
        Popula o banco de dados com a quantidade desejada de registros.

        Args:
            num_usuarios: Número de usuários a serem gerados
            num_livros: Número de livros a serem gerados
            num_emprestimos: Número de empréstimos a serem gerados
        """
        print("Iniciando populamento do banco de dados...")
        self.create_schema()

        if clear:
            print("Limpando o banco de dados...")
            self.limpar_banco()
            print("Banco de dados limpo.")

        usuarios_inseridos = self.inserir_usuarios(num_usuarios)
        print(f"Total de {usuarios_inseridos} usuários inseridos.")

        livros_inseridos = self.inserir_livros(num_livros)
        print(f"Total de {livros_inseridos} livros inseridos.")

        emprestimos_inseridos = self.inserir_emprestimos(num_emprestimos)
        print(f"Total de {emprestimos_inseridos} empréstimos inseridos.")

        print("Populamento do banco de dados concluído!")

        return {
            "usuarios": usuarios_inseridos,
            "livros": livros_inseridos,
            "emprestimos": emprestimos_inseridos,
        }

    def limpar_banco(self):
        """
        Limpa todas as tabelas do banco de dados.
        """
        print("Limpando o banco de dados...")

        # SQL para limpar as tabelas
        sql = """
        TRUNCATE TABLE emprestimos, livros, usuarios RESTART IDENTITY CASCADE;
        """

        execute_query(sql)

        print("Banco de dados limpo com sucesso.")
