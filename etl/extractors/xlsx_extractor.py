import os
import pandas as pd
import petl as etl
class XLSXExtractor:
    def __init__(self, file_path):
        """
        Inicializa con la ruta al archivo Excel.
        file_path: Ruta del archivo Excel.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"El archivo {file_path} no existe.")
        self.file_path = file_path

    def read_sheet(self, sheet_name=None, **kwargs):
        """
        Lee una hoja específica o todas las hojas de un archivo Excel.
        
        :param sheet_name: Nombre de la hoja a leer, Si es None, lee todas las hojas.
        :param kwargs: Parámetros adicionales para la función pd.read_excel
        :return: DataFrame o diccionario de DataFrames.
        """
        try:
            if sheet_name:
                # Leer una hoja específica
                data = pd.read_excel(self.file_path, sheet_name=sheet_name, **kwargs)
                print(f"Hoja '{sheet_name}' leída exitosamente.")
                return data
            else:
                # Leer todas las hojas y devolver un diccionario
                all_sheets = pd.read_excel(self.file_path, sheet_name=None, **kwargs)
                print("Archivo leído exitosamente con todas las hojas.")
                return all_sheets
        except Exception as e:
            print(f"Error al leer el archivo Excel: {e}")
            raise

    def get_sheet_names(self):
        """
        Obtiene los nombres de todas las hojas en el archivo Excel.
        """
        try:
            xls = pd.ExcelFile(self.file_path)
            return xls.sheet_names
        except Exception as e:
            print(f"Error al obtener los nombres de las hojas: {e}")
            raise

    def preview_data(self, sheet_name=None, n=5, **kwargs):
        """
        Muestra las primeras n filas de una hoja específica o todas las hojas.
        
        sheet_name: Nombre de la hoja. Si es None, muestra las primeras filas de todas las hojas.
        Número de filas a mostrar.
        Parámetros adicionales para pd.read_excel.
        """
        try:
            data = self.read_sheet(sheet_name, **kwargs)
            print(data.head(n))
        except Exception as e:
            print(f"Error al previsualizar los datos: {e}")
            raise

    def toxlsx(self, df, filename=None, sheet_name="Sheet1", write_header=True, mode="replace"):
            """
            Guarda un DataFrame o tabla
            
            df: El DataFrame o tabla a guardar.
            filename: Nombre del archivo Excel. Si es None, se usará el archivo original.
            sheet_name: Nombre de la hoja donde se guardarán los datos.
            write_header: Si True, escribe los nombres de las columnas como encabezado.
            mode: Modo de operación en el archivo Excel (replace, overwrite, add).
            """
            if filename is None:
                filename = self.file_path  # Usar el archivo original si no se proporciona otro.

            if isinstance(df, pd.DataFrame):
        # Verificar si hay columnas "Unnamed"
                if df.columns.str.contains("Unnamed").any():
                    df.columns = df.iloc[0]  # Usar la primera fila como encabezados
                    df = df[1:]  # Eliminar la fila que ahora es el encabezado
                    df = df.reset_index(drop=True)  # Reiniciar los índices

            table = etl.fromdataframe(df)
            # Usar la función toxlsx para guardar la tabla en el archivo Excel
            try:
                etl.toxlsx(table, filename, sheet=sheet_name, write_header=write_header, mode=mode)
                print(f"Datos guardados en el archivo '{filename}', hoja '{sheet_name}'.")
            except Exception as e:
                print(f"Error al guardar los datos en el archivo Excel: {e}")
                raise