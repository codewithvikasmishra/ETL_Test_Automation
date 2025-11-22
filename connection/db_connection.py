import pyodbc
from connection.db_config import get_decoded_credentials

class MSSQLConnection:
    def __init__(self, database, server=None, username=None, password=None, auth_type="windows"):
        """
        auth_type = "windows"  → Windows Authentication
        auth_type = "sql"      → SQL Server Authentication
        """
        self.database = database

    def get_connection(self):
        server, username, password, auth_type = get_decoded_credentials()
        print("server",server)
        if auth_type == "windows":
            conn_str = f"""
                DRIVER={{ODBC Driver 17 for SQL Server}};
                SERVER={server};
                DATABASE={self.database};
                Trusted_Connection=yes;
                Encrypt=no;
            """
        elif auth_type == "sql":
            conn_str = f"""
                DRIVER={{ODBC Driver 17 for SQL Server}};
                SERVER={server};
                DATABASE={self.database};
                UID={username};
                PWD={password};
                Encrypt=no;
            """
        else:
            raise ValueError("auth_type must be 'windows' or 'sql'")

        try:
            connection = pyodbc.connect(conn_str)
            print("✅ Connected Successfully!")
            return connection
        except Exception as e:
            print("❌ Connection Failed:", e)
            return None
