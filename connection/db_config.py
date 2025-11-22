import os
import configparser
from _secure.crypto_utils import decode_value

# # Only import crypto utils if encryption is actually used
# try:
#     from _secure.crypto_utils import decode_value
#     encryption_available = True
# except ModuleNotFoundError:
#     encryption_available = False


def load_db_config():
    config = configparser.ConfigParser()

    # Get absolute path to config.ini
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ini_path = os.path.join(base_dir, "config", "config.ini")

    if not os.path.exists(ini_path):
        raise FileNotFoundError(f"config.ini not found at {ini_path}")

    config.read(ini_path)

    server = config["SQL Server"]["server"]
    username = config["SQL Server"].get("username")
    password = config["SQL Server"].get("password")
    auth_type = config["SQL Server"].get("authentication", "windows").lower()

    return {
        "server": server,
        "username": username,
        "password": password,
        "authentication": auth_type
    }


def get_decoded_credentials():
    """
    Read encoded values from config.ini and decode using your crypto utils.
    This function only works if encryption is actually enabled.
    """

    # if not encryption_available:
    #     raise ImportError(
    #         "decode_value() not available. Ensure _secure/crypto_utils.py exists."
    #     )

    cfg = load_db_config()

    # Encoded values from config.ini
    enc_server = cfg["server"]
    enc_username = cfg["username"]
    enc_password = cfg["password"]
    auth_type = cfg["authentication"]

    # Decode only when needed
    server = decode_value(enc_server)
    username = decode_value(enc_username)
    password = decode_value(enc_password)

    return server, username, password, auth_type
# print(get_decoded_credentials())