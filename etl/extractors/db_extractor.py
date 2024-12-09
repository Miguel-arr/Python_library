from sqlalchemy import create_engine
import pandas as pd

class DB_Loader:

    def __init__(self, db_type, password, database, host="localhost", user="root", port=None):
        self.db_type = db_type.lower()
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port or (3306 if self.db_type == "mysql" else 5432)  # Puertos por defecto
        self.engine = None

    def connect(self):
        try:
            if self.db_type == "mysql":
                self.engine = create_engine(
                    f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
                )
            elif self.db_type == "postgresql":
                self.engine = create_engine(
                    f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
                )
            else:
                raise ValueError("Tipo de base de datos no soportado. Usa 'mysql' o 'postgresql'.")

            # Prueba de conexión
            with self.engine.connect() as connection:
                print(f"Conexión exitosa a {self.db_type.upper()} en la base de datos '{self.database}'.")
        except Exception as e:
            print(f"Error al conectar a la base de datos: {e}")
            raise

    def close_connection(self):
        if self.engine:
            self.engine.dispose()
            print("Conexión cerrada.")

    def load_data(self, dataframe, table_name, if_exists="append"):
        
        
        try:
            if self.engine is None:
                raise ValueError("No hay conexión activa. Llame a la funcion `connect` primero.")

            dataframe.to_sql(name=table_name, con=self.engine, if_exists=if_exists, index=False)
            print(f"Datos cargados exitosamente en la tabla '{table_name}'.")
        except Exception as e:
            print(f"Error al cargar datos: {e}")
            raise

    def execute_query(self, query):
       
        try:
            if self.engine is None:
                raise ValueError("No hay conexión activa. Llame a la funcion `connect` primero.")

            with self.engine.connect() as connection:
                result = pd.read_sql_query(query, connection)
                print("Consulta ejecutada con éxito.")
                return result
        except Exception as e:
            print(f"Error al eliminar la tabla: {e}")
            raise







