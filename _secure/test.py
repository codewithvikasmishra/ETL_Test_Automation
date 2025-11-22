import base64

def encode_value(value: str) -> str:
    """Encode a string to Base64."""
    return base64.b64encode(value.encode()).decode()

def decode_value(encoded: str) -> str:
    """Decode a Base64 encoded string."""
    return base64.b64decode(encoded).decode()


# Example usage
original = "DESKTOP-KTAV55P\SQLEXPRESS"
encoded = encode_value(original)
decoded = decode_value(encoded)

print("Original:", original)
print("Encoded:", encoded)
print("Decoded:", decoded)
