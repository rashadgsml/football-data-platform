from football_platform.checksums import calculate_sha256


def test_calculate_sha256_returns_expected_checksum():
    content = b"football data"

    assert calculate_sha256(content) == (
        "ec7d8d6f9075cef0509bff297c0626807b93b382da60452a4102004f8b63986f"
    )
