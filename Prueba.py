from etl.extractors.db_extractor import DB_Loader  # Ajusta la ruta si es necesario

def test_mysql_connection():
    db_params = {
        "db_type": "mysql",
        "password": "",           # Contraseña de tu base de datos
        "database": "ventas"     # Nombre de la base de datos
              # Puerto de MySQL (opcional si usas el predeterminado)
    }

    # Crear una instancia de DB_Loader
    db_loader = DB_Loader(**db_params)

    try:
        # Conectar a la base de datos
        db_loader.connect()

        # Ejecutar una consulta para extraer todos los datos de la tabla `pedido`
        query = "SELECT * FROM pedido;"
        data = db_loader.execute_query(query)

         #Mostrar los resultados como DataFrame
        print("Datos extraídos de la tabla 'pedido':")
        print(data)

    except Exception as e:
        print(f"Error durante la prueba: {e}")
    finally:
        # Cerrar la conexión a la base de datos
        db_loader.close_connection()


# Ejecutar la prueba
if __name__ == "__main__":
    test_mysql_connection()
