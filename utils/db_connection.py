import os
import psycopg2
from psycopg2 import pool
from configparser import ConfigParser
import pandas as pd

class DatabaseConnection:
    """
    Classe para gerenciar conexões com o banco de dados PostgreSQL.
    """
    
    _connection_pool = None
    
    @classmethod
    def init_connection_pool(cls, config_file='database.ini', section='postgresql'):
        """
        Inicializa o pool de conexões usando os parâmetros do arquivo de configuração.
        """
        if cls._connection_pool is None:
            params = cls._get_db_params(config_file, section)
            
            cls._connection_pool = psycopg2.pool.SimpleConnectionPool(
                1,  # min_connections
                10, # max_connections
                **params
            )
            
            print("Pool de conexão inicializado com sucesso")
    
    @classmethod
    def _get_db_params(cls, config_file='database.ini', section='postgresql'):
        """
        Lê os parâmetros de conexão do arquivo de configuração.
        """
        parser = ConfigParser()
        config_path = os.path.join(os.path.dirname(__file__), '..', config_file)
        parser.read(config_path)
        
        db_params = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db_params[param[0]] = param[1]
        else:
            raise Exception(f'Seção {section} não encontrada no arquivo {config_file}')
        
        return db_params
    
    @classmethod
    def get_connection(cls):
        """
        Retorna uma conexão do pool.
        """
        if cls._connection_pool is None:
            cls.init_connection_pool()
        
        return cls._connection_pool.getconn()
    
    @classmethod
    def return_connection(cls, connection):
        """
        Devolve uma conexão ao pool.
        """
        cls._connection_pool.putconn(connection)
    
    @classmethod
    def close_all_connections(cls):
        """
        Fecha todas as conexões no pool.
        """
        if cls._connection_pool is not None:
            cls._connection_pool.closeall()
            cls._connection_pool = None


def execute_query(query, params=None):
    """
    Executa uma consulta SQL e retorna o resultado.
    """
    conn = None
    try:
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        
        try:
            result = cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]
            return pd.DataFrame(result, columns=cols)
        except Exception:
            # Não há resultados a retornar (ex: INSERT, UPDATE, DELETE)
            conn.commit()
            return cursor.rowcount
        
    except Exception as e:
        print(f"Erro ao executar consulta: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            DatabaseConnection.return_connection(conn)


def execute_many(query, params_list):
    """
    Executa uma consulta SQL múltiplas vezes com diferentes parâmetros.
    Útil para inserções em lote.
    """
    conn = None
    try:
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor()
        cursor.executemany(query, params_list)
        conn.commit()
        return cursor.rowcount
    except Exception as e:
        print(f"Erro ao executar consulta múltipla: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            DatabaseConnection.return_connection(conn)


def fetch_dataframe(query, params=None):
    """
    Executa uma consulta SQL e retorna os resultados como um DataFrame do pandas.
    """
    return execute_query(query, params)
