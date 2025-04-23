import pandas as pd
from tabulate import tabulate
from etl.transformer.basics_transformer import BasicsTransformOperations

class DataSelect:


    hd = BasicsTransformOperations


    @staticmethod
    def head2(df, n=5):
        """Devuelve las primeras n filas del DataFrame en formato tabla."""
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not isinstance(n, int) or n < 0:
                raise ValueError("n debe ser un entero positivo")
            
            df_head = df.head(n)
            return tabulate(df_head, headers="keys", tablefmt="fancy_grid", showindex=False)
        except Exception as e:
            print(f"Error al obtener las primeras {n} filas: {e}") 
            raise

    @staticmethod
    def filter_by_operation(df, field, value, op, complement=False, show=0):
        """Selecciona filas donde la operación op aplicada al campo y valor sea True."""
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not isinstance(field, str):
                raise TypeError("field debe ser un string")
            if not callable(op):
                raise TypeError("op debe ser una función callable")
            if not isinstance(complement, bool):
                raise TypeError("complement debe ser un booleano")
            if not isinstance(show, int) or show < -1:
                raise ValueError("show debe ser un entero mayor o igual a -1")

            if field not in df.columns:
                raise ValueError(f"El campo '{field}' no existe en el DataFrame.")
            
            mask = df[field].apply(lambda x: op(x, value))
            result = df[~mask] if complement else df[mask]
            
            if show:
                print(DataSelect.head2(result, show if show > 0 else len(result)))
            
            return result
        except Exception as e:
            print(f"Error en select_op: {e}")
            raise

    @staticmethod
    def filter_equal(df, field, value, complement=False, show=0):
        """Selecciona filas donde el campo es igual al valor."""
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not isinstance(field, str):
                raise TypeError("field debe ser un string")
            if not isinstance(complement, bool):
                raise TypeError("complement debe ser un booleano")
            if not isinstance(show, int) or show < -1:
                raise ValueError("show debe ser un entero mayor o igual a -1")
            
            if field not in df.columns:
                raise ValueError(f"El campo '{field}' no existe en el DataFrame.")

            mask = df[field] == value
            result = df[~mask] if complement else df[mask]
            
            if show:
                print(DataSelect.head2(result, show if show > 0 else len(result)))
            
            return result
        except Exception as e:
            print(f"Error en select_eq: {e}")
            raise
    
    # Métodos adicionales con validaciones
    @staticmethod
    def filter_not_equal(df, field, value, complement=False, show=0):
        return DataSelect.filter_equal(df, field, value, not complement, show)

    @staticmethod
    def filter_in_range(df, field, minv, maxv, complement=False, show=0):
        """Selecciona filas donde el campo está entre minv y maxv (ambos incluidos)."""
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not isinstance(field, str):
                raise TypeError("field debe ser un string")
            if not isinstance(minv, (int, float)) or not isinstance(maxv, (int, float)):
                raise TypeError("minv y maxv deben ser numéricos")
            if not isinstance(complement, bool):
                raise TypeError("complement debe ser un booleano")
            if not isinstance(show, int) or show < -1:
                raise ValueError("show debe ser un entero mayor o igual a -1")

            if field not in df.columns:
                raise ValueError(f"El campo '{field}' no existe en el DataFrame.")
            
            mask = (df[field] >= minv) & (df[field] <= maxv)
            result = df[~mask] if complement else df[mask]
            
            if show:
                print(DataSelect.head2(result, show if show > 0 else len(result)))
            
            return result
        except Exception as e:
            print(f"Error en select_range_open: {e}")
            raise
    
    @staticmethod
    def filter_contains(df, field, value, complement=False, show=0):
        """Selecciona filas donde el campo contiene el valor dado."""
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not isinstance(field, str):
                raise TypeError("field debe ser un string")
            if not isinstance(value, str):
                raise TypeError("value debe ser un string")
            if not isinstance(complement, bool):
                raise TypeError("complement debe ser un booleano")
            if not isinstance(show, int) or show < -1:
                raise ValueError("show debe ser un entero mayor o igual a -1")

            if field not in df.columns:
                raise ValueError(f"El campo '{field}' no existe en el DataFrame.")
            
            mask = df[field].astype(str).str.contains(value, na=False)
            result = df[~mask] if complement else df[mask]
            
            if show:
                print(DataSelect.head2(result, show if show > 0 else len(result)))
            
            return result
        except Exception as e:
            print(f"Error en select_contains: {e}")
            raise


    @staticmethod
    def filter_in_list(df, field, values, complement=False, show=0):
        """Selecciona filas donde el campo está en la lista de valores."""
        try:
            if field not in df.columns:
                raise ValueError(f"El campo '{field}' no existe en el DataFrame.")

            if not isinstance(values, (list, set, tuple)):
                raise ValueError("El parámetro 'values' debe ser una lista, conjunto o tupla.")

            mask = df[field].isin(values)
            result = df[~mask] if complement else df[mask]

            if show:
                print(DataSelect.head2(result, show if show > 0 else len(result)))

            return result
        except Exception as e:
            print(f"Error en select_in: {e}")
            raise

    @staticmethod
    def filter_is_null(df, field, complement=False, show=0):
        """Selecciona filas donde el campo es None (NaN)."""
        try:
            if field not in df.columns:
                raise ValueError(f"El campo '{field}' no existe en el DataFrame.")

            mask = df[field].isna()
            result = df[~mask] if complement else df[mask]

            if show:
                print(DataSelect.head2(result, show if show > 0 else len(result)))

            return result
        except Exception as e:
            print(f"Error en select_none: {e}")
            raise

    @staticmethod
    def select_not_none(df, field, complement=False, show=0):
        """Selecciona filas donde el campo no es None (NaN)."""
        try:
            if field not in df.columns:
                raise ValueError(f"El campo '{field}' no existe en el DataFrame.")

            mask = df[field].notna()
            result = df[~mask] if complement else df[mask]

            if show:
                print(DataSelect.head2(result, show if show > 0 else len(result)))

            return result
        except Exception as e:
            print(f"Error en select_not_none: {e}")
            raise
