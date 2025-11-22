from connection.db_connection import MSSQLConnection

db = MSSQLConnection(
    database="Test"
)

conn = db.get_connection()
cursor = conn.cursor()
cursor.execute("SELECT TOP 5 * FROM login_data")
for row in cursor.fetchall():
    print(row)