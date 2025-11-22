import base64

def decode_value(encoded):
    return base64.b64decode(encoded).decode()
