import pandas as pd

class TransformOperations:

    @staticmethod
    def add_column(df, new_column_name, lambda_func):
        """
        Agrega una nueva columna calculada a un DataFrame.

        :param df: DataFrame original
        :param new_column_name: Nombre de la nueva columna
        :param lambda_func: Función que define el cálculo para cada fila
        :return: DataFrame con la nueva columna
        """
        try:
            df[new_column_name] = df.apply(lambda_func, axis=1)
            return df
        except Exception as e:
            print(f"Error al agregar la columna: {e}")
            raise

    @staticmethod
    def filter_rows(df, lambda_func):
        """
        Filtra las filas de un DataFrame según una condición.

        :param df: DataFrame original
        :param lambda_func: Función que define la condición para cada fila
        :return: DataFrame filtrado
        """
        try:
            filtered_df = df[df.apply(lambda_func, axis=1)]
            return filtered_df
        except Exception as e:
            print(f"Error al filtrar las filas: {e}")
            raise

    @staticmethod
    def drop_columns(df, columns_to_drop):
        """
        Elimina las columnas especificadas de un DataFrame.

        :param df: DataFrame original
        :param columns_to_drop: Lista de nombres de columnas a eliminar
        :return: DataFrame sin las columnas eliminadas
        """
        try:
            df = df.drop(columns=columns_to_drop)
            return df
        except Exception as e:
            print(f"Error al eliminar las columnas: {e}")
            raise

    @staticmethod
    def left_join(df1, df2, on):
        """
        Realiza un left join entre dos DataFrames.

        :param df1: Primer DataFrame
        :param df2: Segundo DataFrame
        :param on: Columna o índice sobre el cual hacer el join
        :return: DataFrame resultante de la unión
        """
        try:
            result_df = pd.merge(df1, df2, how='left', on=on)
            return result_df
        except Exception as e:
            print(f"Error al realizar el left join: {e}")
            raise

    @staticmethod
    def group_by_sum(df, by, column):
        """
        Agrupa un DataFrame por una columna y suma los valores de otra columna.

        :param df: DataFrame original
        :param by: Columna sobre la cual agrupar
        :param column: Columna cuyos valores se sumarán
        :return: DataFrame con los valores agrupados y sumados
        """
        try:
            grouped_df = df.groupby(by)[column].sum().reset_index()
            return grouped_df
        except Exception as e:
            print(f"Error al agrupar y sumar: {e}")
            raise

    @staticmethod
    def rename_columns(df, column_mapping):
        """
        Renombra las columnas de un DataFrame.

        :param df: DataFrame original
        :param column_mapping: Diccionario con los nombres antiguos y nuevos de las columnas
        :return: DataFrame con las columnas renombradas
        """
        try:
            df = df.rename(columns=column_mapping)
            return df
        except Exception as e:
            print(f"Error al renombrar las columnas: {e}")
            raise

    @staticmethod
    def apply_to_column(df, column, func):
        """
        Aplica una función personalizada a una columna específica.

        :param df: DataFrame original
        :param column: Columna a la que se aplicará la función
        :param func: Función a aplicar
        :return: DataFrame con la columna transformada
        """
        try:
            df[column] = df[column].apply(func)
            return df
        except Exception as e:
            print(f"Error al aplicar la función a la columna: {e}")
            raise

    @staticmethod
    def sort_by(df, columns, ascending=True):
        """
        Ordena un DataFrame según las columnas especificadas.

        :param df: DataFrame original
        :param columns: Columna(s) sobre la cual ordenar
        :param ascending: Si es True, ordena en orden ascendente (por defecto)
        :return: DataFrame ordenado
        """
        try:
            sorted_df = df.sort_values(by=columns, ascending=ascending)
            return sorted_df
        except Exception as e:
            print(f"Error al ordenar las filas: {e}")
            raise

    @staticmethod
    def drop_duplicates(df, subset=None):
        """
        Elimina filas duplicadas en un DataFrame.

        :param df: DataFrame original
        :param subset: Lista de columnas por las cuales verificar duplicados (opcional)
        :return: DataFrame sin duplicados
        """
        try:
            df_no_duplicates = df.drop_duplicates(subset=subset)
            return df_no_duplicates
        except Exception as e:
            print(f"Error al eliminar duplicados: {e}")
            raise

    @staticmethod
    def replace_values(df, column, old_value, new_value):
        """
        Reemplaza valores en una columna de un DataFrame.

        :param df: DataFrame original
        :param column: Columna en la que se realizará el reemplazo
        :param old_value: Valor antiguo que será reemplazado
        :param new_value: Valor nuevo
        :return: DataFrame con los valores reemplazados
        """
        try:
            df[column] = df[column].replace(old_value, new_value)
            return df
        except Exception as e:
            print(f"Error al reemplazar valores en la columna: {e}")
            raise
