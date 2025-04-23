import mysql.connector

conexion = mysql.connector.connect(
    host="localhost",
    user="root",
    password="admin",  # <-- escribe aquí tu contraseña si tienes
    database="ventas",  # Ya que en la tabla dice "ventas.cliente"
    port=3306
)

cursor = conexion.cursor()
cursor.execute("SELECT * FROM cliente LIMIT 5")

for fila in cursor.fetchall():
    print(fila)

cursor.close()
conexion.close()
