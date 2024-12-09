import os
import pandas as pd
import petl as etl


class CSVExtractor:
    def __init__(self, file_path):
        """
        Inicializa con la ruta al archivo CSV.
        
        :param file_path: Ruta del archivo CSV.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"El archivo {file_path} no existe.")
        self.file_path = file_path

    def read_csv(self, **kwargs):
        """
        Lee el archivo CSV y lo retorna como un DataFrame.
        
        :param kwargs: Parámetros adicionales para la función pd.read_csv (por ejemplo, sep, dtype, usecols).
        :return: DataFrame con los datos del archivo CSV.
        """
        try:
            data = pd.read_csv(self.file_path, **kwargs)
            print("Archivo CSV leído exitosamente.")
            return data
        except Exception as e:
            print(f"Error al leer el archivo CSV: {e}")
            raise

    def preview_data(self, n=5, **kwargs):
        """
        Muestra las primeras n filas del archivo CSV.
        
        :param n: Número de filas a mostrar.
        :param kwargs: Parámetros adicionales para pd.read_csv.
        """
        try:
            data = self.read_csv(**kwargs)
            print(data.head(n))
        except Exception as e:
            print(f"Error al previsualizar los datos: {e}")
            raise

    def save_csv(self, df, output_path, **kwargs):
        """
        Guarda un DataFrame como un archivo CSV.
        
        :param df: DataFrame a guardar.
        :param output_path: Ruta donde se guardará el archivo CSV.
        :param kwargs: Parámetros adicionales para la función pd.to_csv (por ejemplo, index, sep).
        """
        try:
            df.to_csv(output_path, **kwargs)
            print(f"Archivo CSV guardado en: {output_path}")
        except Exception as e:
            print(f"Error al guardar el archivo CSV: {e}")
            raise

    def tocvs_with_petl(self, df, output_path, **kwargs):
        """
        Guarda un DataFrame en un archivo CSV utilizando PETL.
        
        :param df: DataFrame a guardar.
        :param output_path: Ruta donde se guardará el archivo CSV.
        :param kwargs: Parámetros adicionales para petl.tocsv.
        """
        try:
            table = etl.fromdataframe(df)
            etl.tocsv(table, output_path, **kwargs)
            print(f"Datos guardados en el archivo CSV: {output_path}")
        except Exception as e:
            print(f"Error al guardar el archivo CSV con PETL: {e}")
            raise


# Ejemplo de uso:
# Crear un objeto del extractor
extractor = CSVExtractor("ruta/a/tu/archivo.csv")

# Leer el archivo CSV completo
data = extractor.read_csv()

# Previsualizar los primeros 5 registros
extractor.preview_data(n=5)

# Guardar los datos en otro archivo CSV
extractor.save_csv(data, "ruta/a/archivo_salida.csv", index=False)
