class TransformOperations:
    @staticmethod
    def filter_rows(data, condition):
        """
        Filtra las filas del DataFrame según una condición.

        Args:
            data (DataFrame): El conjunto de datos de entrada.
            condition (function): Función que devuelve True o False para cada fila.

        Returns:
            DataFrame: Conjunto de datos filtrado.
        """
        return data[data.apply(condition, axis=1)]

    @staticmethod
    def add_column(data, column_name, value_function):
        """
        Agrega una nueva columna al DataFrame.

        Args:
            data (DataFrame): El conjunto de datos de entrada.
            column_name (str): Nombre de la nueva columna.
            value_function (function): Función que genera valores para la nueva columna.

        Returns:
            DataFrame: Conjunto de datos con la nueva columna agregada.
        """
        data[column_name] = data.apply(value_function, axis=1)
        return data

    @staticmethod
    def drop_columns(data, columns):
        """
        Elimina una o más columnas del DataFrame.

        Args:
            data (DataFrame): El conjunto de datos de entrada.
            columns (list): Lista de nombres de columnas a eliminar.

        Returns:
            DataFrame: Conjunto de datos sin las columnas especificadas.
        """
        return data.drop(columns=columns)

    @staticmethod
    def rename_columns(data, column_mapping):
        """
        Renombra columnas en el DataFrame.

        Args:
            data (DataFrame): El conjunto de datos de entrada.
            column_mapping (dict): Diccionario con el mapeo {nombre_actual: nuevo_nombre}.

        Returns:
            DataFrame: Conjunto de datos con las columnas renombradas.
        """
        return data.rename(columns=column_mapping)

    @staticmethod
    def sort_data(data, by, ascending=True):
        """
        Ordena el DataFrame por una o más columnas.

        Args:
            data (DataFrame): El conjunto de datos de entrada.
            by (str or list): Columna(s) por las que se debe ordenar.
            ascending (bool): Orden ascendente (True) o descendente (False).

        Returns:
            DataFrame: Conjunto de datos ordenado.
        """
        return data.sort_values(by=by, ascending=ascending)

    @staticmethod
    def aggregate(data, key, aggregation):
        """
        Agrega datos agrupándolos por una o más claves.

        Args:
            data (DataFrame): El conjunto de datos de entrada.
            key (str or list): Clave(s) para agrupar.
            aggregation (dict): Diccionario {columna: función_de_agregación}.

        Returns:
            DataFrame: Conjunto de datos agregado.
        """
        return data.groupby(key).agg(aggregation).reset_index()

    @staticmethod
    def merge_data(data1, data2, on, how="inner"):
        """
        Combina dos DataFrames.

        Args:
            data1 (DataFrame): Primer conjunto de datos.
            data2 (DataFrame): Segundo conjunto de datos.
            on (str or list): Columna(s) para combinar.
            how (str): Tipo de combinación ('inner', 'outer', 'left', 'right').

        Returns:
            DataFrame: Conjunto de datos combinado.
        """
        return data1.merge(data2, on=on, how=how)

    @staticmethod
    def pivot_data(data, index, columns, values):
        """
        Reorganiza el DataFrame en formato pivot.

        Args:
            data (DataFrame): El conjunto de datos de entrada.
            index (str or list): Columna(s) para usar como índice.
            columns (str): Columna para usar como encabezado.
            values (str): Columna para usar como valores.

        Returns:
            DataFrame: Conjunto de datos reorganizado en formato pivot.
        """
        return data.pivot(index=index, columns=columns, values=values)
