import pandas as pd
from tabulate import tabulate

class TransformOperations:

    @staticmethod
    def head(df, n=5): #funcion auxiliar para retornar un tamaño defino del dataframe con tabulate 
        try:
            df_head = df.head(n)
            return tabulate(df_head, headers="keys", tablefmt="fancy_grid", showindex=False)
        except Exception as e:
            print(f"Error al obtener las primeras {n} filas: {e}")
            raise


    @staticmethod
    def left_join(df1, df2, on, show=0): #  Realiza un left join entre dos DataFrames.
        try:
            if not isinstance(df1, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not isinstance(df2, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")

            result_df = pd.merge(df1, df2, how='left', on=on)

            if show > 0:
                print(TransformOperations.head(result_df, show))
            elif show == -1:
                print(TransformOperations.head(result_df, len(result_df)))

            return result_df

        except Exception as e:
            print(f"Error al realizar el left join: {e}")
            raise


    
    @staticmethod
    def right_join(df1, df2, on, show = 0): # Realiza un Right join entre dos DataFrames.
        try:
            if not isinstance(df1, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not isinstance(df2, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")

            result_df = pd.merge(df1, df2, how='right', on=on)

            if show > 0:
                print(TransformOperations.head(result_df, show))
            elif show == -1:
                print(TransformOperations.head(result_df, len(result_df)))

            
            return result_df
        except Exception as e:
            print(f"Error al realizar el right join: {e}")
            raise
    
    @staticmethod
    def inner_join(df1, df2, on, show = 0): # Realiza un inner join entre dos DataFrames
    
        try:
            if not isinstance(df1, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not isinstance(df2, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")

            result_df = pd.merge(df1, df2, how='inner', on=on)

            if show > 0:
                print(TransformOperations.head(result_df, show))
            elif show == -1:
                print(TransformOperations.head(result_df, len(result_df)))

            return result_df
        except Exception as e:  
            print(f"Error al realizar el inner join: {e}")
            raise
    
    @staticmethod
    def outer_join(df1, df2, on, show = 0):  #Realiza un outer join entre dos DataFrames.
        
        try:
            if not isinstance(df1, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not isinstance(df2, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")

            result_df = pd.merge(df1, df2, how='inner', on=on)

            if show > 0:
                print(TransformOperations.head(result_df, show))
            elif show == -1:
                print(TransformOperations.head(result_df, len(result_df)))

            result_df = pd.merge(df1, df2, how='outer', on=on)
            return result_df
        except Exception as e:
            print(f"Error al realizar el outer join: {e}")
            raise


    @staticmethod
    def group_by_sum(df, by, column): #Agrupa un DataFrame por una columna y suma los valores de otra columna

        try:
            grouped_df = df.groupby(by)[column].sum().reset_index()
            return grouped_df
        except Exception as e:
            print(f"Error al agrupar y sumar: {e}")
            raise

   
    @staticmethod
    def apply_to_column(df, column, func):
       
        try:
            df[column] = df[column].apply(func)
            return df
        except Exception as e:
            print(f"Error al aplicar la función a la columna: {e}")
            raise

    @staticmethod
    def sort_by(df, columns, ascending=True): # Ordena un DataFrame según las columnas especificadas.

        try:
            sorted_df = df.sort_values(by=columns, ascending=ascending)
            return sorted_df
        except Exception as e:
            print(f"Error al ordenar las filas: {e}")
            raise

    @staticmethod
    def drop_duplicates(df, subset=None): # Elimina filas duplicadas en un DataFrame.

        try:
            df_no_duplicates = df.drop_duplicates(subset=subset)
            return df_no_duplicates
        except Exception as e:
            print(f"Error al eliminar duplicados: {e}")
            raise

    @staticmethod
    def replace_values(df, column, old_value, new_value):# Reemplaza valores en una columna de un DataFrame.

        try:
            df[column] = df[column].replace(old_value, new_value)
            return df
        except Exception as e:
            print(f"Error al reemplazar valores en la columna: {e}")
            raise
    