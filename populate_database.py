import time
from utils.db_connection import DatabaseConnection
from utils.data_generator import BibliotecaDataGenerator

def main():
    """
    Script principal para popular o banco de dados para o trabalho de índices
    """
    # Inicializa a conexão com o banco de dados
    DatabaseConnection.init_connection_pool()
    
    try:
        print("Iniciando geração de dados para o trabalho de índices em PostgreSQL...")
        
        # Cria o gerador de dados
        gerador = BibliotecaDataGenerator(seed=42)
        
        # Registra o tempo de início
        inicio = time.time()
        
        # Popula o banco de dados
        # Você pode ajustar esses números conforme necessário
        resultado = gerador.popular_banco(
            num_usuarios=100000,    # 10 mil usuários
            num_livros=200000,      # 20 mil livros
            num_emprestimos=500000,  # 50 mil empréstimos
            clear=True
        )
        
        # Registra o tempo de término
        fim = time.time()
        tempo_total = fim - inicio
        
        # Exibe um resumo
        print("\n" + "="*50)
        print("RESUMO DA GERAÇÃO DE DADOS")
        print("="*50)
        print(f"Usuários inseridos: {resultado['usuarios']}")
        print(f"Livros inseridos: {resultado['livros']}")
        print(f"Empréstimos inseridos: {resultado['emprestimos']}")
        print(f"Total de registros: {sum(resultado.values())}")
        print(f"Tempo total de execução: {tempo_total:.2f} segundos")
        print("="*50)
        
    finally:
        # Fecha todas as conexões do pool
        DatabaseConnection.close_all_connections()

if __name__ == "__main__":
    main()
