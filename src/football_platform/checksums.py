import hashlib


def calculate_sha256(content: bytes) -> str:
    """Return the SHA-256 checksum of the supplied bytes."""
    return hashlib.sha256(content).hexdigest()
