import hashlib


# Function to hash a value with SHA-512 and return the first n characters
def hash_value(value, length):
    if value is not None:
        hashed_value = hashlib.sha512(str(value).encode()).hexdigest()
        return hashed_value[:length]
    else:
        return None
