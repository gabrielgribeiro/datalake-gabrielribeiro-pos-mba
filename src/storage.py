"""
Camada de armazenamento.

Expoe a mesma interface para dois destinos:
  - LocalStorage: grava em ./data/datalake (util para desenvolver e testar sem custo AWS)
  - S3Storage:    grava no Amazon S3 via boto3

Assim o pipeline nao precisa saber onde esta gravando: basta trocar
a variavel DATALAKE_TARGET entre "local" e "s3".
"""

from __future__ import annotations

import io
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd

from src.config import settings


class Storage(ABC):
    """Contrato minimo usado pelo pipeline."""

    @abstractmethod
    def write_bytes(self, key: str, data: bytes) -> str: ...

    @abstractmethod
    def read_bytes(self, key: str) -> bytes: ...

    @abstractmethod
    def uri(self, key: str) -> str: ...

    # ------------------------------------------------------------------ helpers
    def write_text(self, key: str, text: str) -> str:
        return self.write_bytes(key, text.encode("utf-8"))

    def write_csv(self, key: str, df: pd.DataFrame) -> str:
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, sep=",", lineterminator="\n")
        return self.write_text(key, buffer.getvalue())

    def read_csv(self, key: str, dtype=None) -> pd.DataFrame:
        return pd.read_csv(io.BytesIO(self.read_bytes(key)), dtype=dtype)

    def write_parquet(self, key: str, df: pd.DataFrame) -> str:
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)
        return self.write_bytes(key, buffer.getvalue())

    def read_parquet(self, key: str) -> pd.DataFrame:
        return pd.read_parquet(io.BytesIO(self.read_bytes(key)), engine="pyarrow")


class LocalStorage(Storage):
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def _path(self, key: str) -> Path:
        return self.root / key

    def write_bytes(self, key: str, data: bytes) -> str:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return str(path)

    def read_bytes(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def uri(self, key: str) -> str:
        return str(self._path(key))


class S3Storage(Storage):
    def __init__(self, bucket: str, region: str) -> None:
        import boto3  # importado aqui para nao exigir boto3 no modo local

        self.bucket = bucket
        self.region = region
        self.client = boto3.client("s3", region_name=region)

    def write_bytes(self, key: str, data: bytes) -> str:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)
        return self.uri(key)

    def read_bytes(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    def ensure_bucket(self) -> None:
        """Cria o bucket caso ele ainda nao exista."""
        from botocore.exceptions import ClientError

        try:
            self.client.head_bucket(Bucket=self.bucket)
            print(f"[storage] bucket ja existe: s3://{self.bucket}")
        except ClientError:
            kwargs = {"Bucket": self.bucket}
            if self.region != "us-east-1":
                kwargs["CreateBucketConfiguration"] = {"LocationConstraint": self.region}
            self.client.create_bucket(**kwargs)
            print(f"[storage] bucket criado: s3://{self.bucket}")


def get_storage() -> Storage:
    if settings.target == "s3":
        return S3Storage(settings.bucket, settings.region)
    return LocalStorage(settings.local_root)
