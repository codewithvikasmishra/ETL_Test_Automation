from db_connection import MSSQLConnection

db = MSSQLConnection(
    server=r"DESKTOP-12345\SQLEXPRESS",
    database="HOSPITAL_DB",
    username="sa",
    password="YourPassword123",
    auth_type="sql"
)

conn = db.get_connection()
cursor = conn.cursor()
cursor.execute("SELECT TOP 5 * FROM doctor")
for row in cursor.fetchall():
    print(row)