"""Encryption helpers for transformed pipeline records."""

import base64
import hashlib
import hmac
import json
import os
from typing import Any

from cryptography.hazmat.primitives import hashes, padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


KEY_ENVIRONMENT_VARIABLE = "PIPELINE_AES_KEY"
KEY_LENGTH_BYTES = 32


def _get_key() -> bytes:
    encoded_key = os.environ.get(KEY_ENVIRONMENT_VARIABLE)

    if not encoded_key:
        raise RuntimeError(
            f"Miljøvariablen {KEY_ENVIRONMENT_VARIABLE} mangler. "
            "Generer en Base64-kodet 32-byte nøgle først."
        )

    try:
        key = base64.urlsafe_b64decode(encoded_key.encode("ascii"))
    except (ValueError, UnicodeEncodeError) as error:
        raise RuntimeError(
            f"{KEY_ENVIRONMENT_VARIABLE} skal være en gyldig Base64-nøgle."
        ) from error

    if len(key) != KEY_LENGTH_BYTES:
        raise RuntimeError(
            f"{KEY_ENVIRONMENT_VARIABLE} skal indeholde en "
            f"{KEY_LENGTH_BYTES}-byte AES-nøgle."
        )

    return key


def _encode(*parts: bytes) -> str:
    return ":".join(
        base64.urlsafe_b64encode(part).decode("ascii")
        for part in parts
    )


def _decode(token: str, expected_parts: int) -> list[bytes]:
    parts = token.split(":")

    if len(parts) != expected_parts:
        raise ValueError("Krypteret data har et ugyldigt format.")

    try:
        return [base64.urlsafe_b64decode(part) for part in parts]
    except (ValueError, UnicodeEncodeError) as error:
        raise ValueError("Krypteret data indeholder ugyldig Base64.") from error


def encrypt_gcm(plaintext: str) -> str:
    """Encrypt plaintext with AES-256-GCM and a fresh nonce."""

    key = _get_key()
    nonce = os.urandom(12)
    ciphertext = AESGCM(key).encrypt(nonce, plaintext.encode("utf-8"), None)
    return f"gcm:{_encode(nonce, ciphertext)}"


def decrypt_gcm(token: str) -> str:
    """Decrypt and authenticate an AES-GCM token."""

    if not token.startswith("gcm:"):
        raise ValueError("Data er ikke markeret som AES-GCM.")

    nonce, ciphertext = _decode(token[4:], 2)
    plaintext = AESGCM(_get_key()).decrypt(nonce, ciphertext, None)
    return plaintext.decode("utf-8")


def encrypt_cbc(plaintext: str) -> str:
    """Encrypt with AES-256-CBC and authenticate with HMAC-SHA256."""

    key = _get_key()
    iv = os.urandom(16)
    padder = padding.PKCS7(algorithms.AES.block_size).padder()
    padded = padder.update(plaintext.encode("utf-8")) + padder.finalize()
    encryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).encryptor()
    ciphertext = encryptor.update(padded) + encryptor.finalize()
    tag = hmac.new(key, iv + ciphertext, hashlib.sha256).digest()
    return f"cbc:{_encode(iv, ciphertext, tag)}"


def decrypt_cbc(token: str) -> str:
    """Verify and decrypt an AES-CBC/HMAC token."""

    if not token.startswith("cbc:"):
        raise ValueError("Data er ikke markeret som AES-CBC.")

    iv, ciphertext, tag = _decode(token[4:], 3)
    expected_tag = hmac.new(
        _get_key(),
        iv + ciphertext,
        hashlib.sha256
    ).digest()

    if not hmac.compare_digest(tag, expected_tag):
        raise ValueError("AES-CBC-integritetskontrollen fejlede.")

    decryptor = Cipher(
        algorithms.AES(_get_key()),
        modes.CBC(iv)
    ).decryptor()
    padded = decryptor.update(ciphertext) + decryptor.finalize()
    unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
    plaintext = unpadder.update(padded) + unpadder.finalize()
    return plaintext.decode("utf-8")


def encrypt_record(record: dict[str, Any]) -> str:
    """Serialize one transformed record and encrypt it with AES-GCM."""

    plaintext = json.dumps(
        record,
        ensure_ascii=True,
        separators=(",", ":"),
        default=str
    )
    return encrypt_gcm(plaintext)


def decrypt_record(token: str) -> dict[str, Any]:
    """Decrypt one record produced by :func:`encrypt_record`."""

    return json.loads(decrypt_gcm(token))


def encrypt_dataframe(dataframe):
    """Return a Spark DataFrame containing only encrypted record values."""

    def encrypt_row(row):
        return (encrypt_record(row.asDict(recursive=True)),)

    return dataframe.rdd.map(encrypt_row).toDF(["encrypted_data"])
