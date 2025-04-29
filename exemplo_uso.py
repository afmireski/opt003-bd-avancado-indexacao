from utils.db_connection import fetch_dataframe, execute_query, DatabaseConnection
import pandas as pd
import matplotlib.pyplot as plt

# Inicie o pool de conexões
DatabaseConnection.init_connection_pool()

# Exemplo de consulta simples
def exemplo_consulta_simples():
    # Substitua pela sua consulta
    query = "SELECT * FROM sua_tabela LIMIT 10"
    df = fetch_dataframe(query)
    print("Exemplo de consulta simples:")
    print(df.head())
    return df

# Exemplo de consulta parametrizada
def exemplo_consulta_parametrizada(limite=5):
    query = "SELECT * FROM sua_tabela WHERE coluna > %s LIMIT %s"
    params = (100, limite)
    df = fetch_dataframe(query, params)
    print("\nExemplo de consulta parametrizada:")
    print(df.head())
    return df

# Exemplo de visualização simples
def exemplo_visualizacao(df):
    if df is not None and not df.empty:
        # Substitua 'coluna1' e 'coluna2' pelos nomes reais das colunas
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.bar(df['coluna1'], df['coluna2'])
        ax.set_xlabel('Coluna 1')
        ax.set_ylabel('Coluna 2')
        ax.set_title('Gráfico de Exemplo')
        plt.tight_layout()
        plt.savefig('grafico_exemplo.png')
        plt.close()
        print("\nGráfico salvo como 'grafico_exemplo.png'")

if __name__ == "__main__":
    try:
        df1 = exemplo_consulta_simples()
        df2 = exemplo_consulta_parametrizada(10)
        exemplo_visualizacao(df1)
    finally:
        # Certifique-se de fechar todas as conexões quando terminar
        DatabaseConnection.close_all_connections()
