from etl.extractors.db_extractor import DB_Loader
from etl.transformer.operaciones_transformer import TransformOperations

def test_mysql_connection():
    db_params = {
        "db_type": "mysql",
        "password": "",           
        "database": "ventas"     
    }

    db_loader = DB_Loader(**db_params)

    try:
        # Conectar a la base de datos
        db_loader.connect()

        # Extraer los datos de las tablas
        print("Extrayendo datos de la tabla cliente...")
        query_clientes = "SELECT * FROM cliente;"
        clientes = db_loader.execute_query(query_clientes)

        print("Extrayendo datos de la tabla comercial...")
        query_comerciales = "SELECT * FROM comercial;"
        comerciales = db_loader.execute_query(query_comerciales)

        print("Extrayendo datos de la tabla pedido...")
        query_pedidos = "SELECT * FROM pedido;"
        pedidos = db_loader.execute_query(query_pedidos)

        # Aplicar todas las transformaciones
        print("Agregando una columna calculada 'nuevo_total'...")
        data = TransformOperations.add_column(pedidos, "nuevo_total", lambda row: row["total"] * 1.2)
        print(data.head())

        print("Filtrando filas donde el total es mayor a 100...")
        data = TransformOperations.filter_rows(data, lambda row: row["total"] > 100)
        print(data.head())

        print("Eliminando columnas innecesarias...")
        data = TransformOperations.drop_columns(data, ["id_comercial"])
        print(data.head())

        print("Renombrando columnas para mayor claridad...")
        data = TransformOperations.rename_columns(data, { "total": "monto_total"})
        print(data.head())

        print("Aplicando una transformación a la columna 'nuevo_total'...")
        data = TransformOperations.apply_to_column(data, "nuevo_total", lambda x: round(x, 2))  # Redondear el nuevo total
        print(data.head())

        print("Ordenando los datos por 'nuevo_total' de forma descendente...")
        data = TransformOperations.sort_by(data, columns=["nuevo_total"], ascending=False)
        print(data.head())

        print("Eliminando filas duplicadas basadas en 'id_cliente'...")
        data = TransformOperations.drop_duplicates(data, subset=["id_cliente"])
        print(data.head())

        print("Reemplazando valores en la columna 'monto_total' donde sea negativo...")
        data = TransformOperations.replace_values(data, "monto_total", -1, 0)  # Reemplazar valores negativos por 0
        print(data.head())

        print("Realizando left join entre 'pedido' y 'cliente'...")
        data = TransformOperations.left_join(data, clientes, on="id")  # Unir por 'id_cliente'
        print(data.head())

        print("Agrupando por cliente y sumando el total de los pedidos...")
        data_grouped = TransformOperations.group_by_sum(data, by="id_cliente", column="monto_total")
        print(data_grouped)

    except Exception as e:
        print(f"Error durante la ejecución: {e}")
    finally:
        db_loader.close_connection()

if __name__ == "__main__":
    test_mysql_connection()
