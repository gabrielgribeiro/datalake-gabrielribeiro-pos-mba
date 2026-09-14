"""
Configuracao central do pipeline.

Todos os parametros sao lidos de variaveis de ambiente (arquivo .env opcional),
para que o mesmo codigo rode em modo LOCAL (simulando o data lake em disco)
ou em modo S3 (Amazon S3 real), sem nenhuma alteracao nos scripts.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _load_dotenv() -> None:
    """Carrega o arquivo .env da raiz do projeto, se existir (sem dependencias externas)."""
    env_file = PROJECT_ROOT / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_dotenv()


@dataclass(frozen=True)
class Settings:
    # ------------------------------------------------------------------ destino
    # "local" -> grava em ./data/datalake  |  "s3" -> grava no bucket informado
    target: str = os.getenv("DATALAKE_TARGET", "local").lower()
    bucket: str = os.getenv("S3_BUCKET", "meu-datalake-mackenzie")
    region: str = os.getenv("AWS_REGION", "us-east-1")
    local_root: Path = PROJECT_ROOT / "data" / "datalake"

    # ------------------------------------------------------------------ particao
    ingest_date: str = os.getenv("INGEST_DATE", date.today().isoformat())

    # ------------------------------------------------- volume da massa simulada
    n_clientes: int = int(os.getenv("N_CLIENTES", "200"))
    n_produtos: int = int(os.getenv("N_PRODUTOS", "60"))
    n_pedidos: int = int(os.getenv("N_PEDIDOS", "2000"))
    taxa_anomalias: float = float(os.getenv("TAXA_ANOMALIAS", "0.12"))
    seed: int = int(os.getenv("SEED", "42"))

    # ------------------------------------------------------------------ athena
    athena_database: str = os.getenv("ATHENA_DATABASE", "datalake_mackenzie")
    athena_workgroup: str = os.getenv("ATHENA_WORKGROUP", "primary")

    # ------------------------------------------------------- prefixos das camadas
    prefix_raw: str = "raw"
    prefix_quarantine: str = "quarantine"
    prefix_processed: str = "processed"
    prefix_gold: str = "gold"
    prefix_athena_results: str = "athena-results"

    # ------------------------------------------------------------------ helpers
    @property
    def base_uri(self) -> str:
        if self.target == "s3":
            return f"s3://{self.bucket}"
        return str(self.local_root)

    @property
    def athena_output(self) -> str:
        return f"s3://{self.bucket}/{self.prefix_athena_results}/"

    def key_raw(self, entidade: str) -> str:
        return f"{self.prefix_raw}/{entidade}/ingest_date={self.ingest_date}/{entidade}.csv"

    def key_quarantine(self) -> str:
        return (
            f"{self.prefix_quarantine}/pedidos_rejeitados/"
            f"data={self.ingest_date}/rejeitados.json"
        )

    def key_silver(self) -> str:
        return (
            f"{self.prefix_processed}/fato_vendas/"
            f"ingest_date={self.ingest_date}/fato_vendas.snappy.parquet"
        )

    def key_gold(self) -> str:
        return (
            f"{self.prefix_gold}/agg_vendas_uf_categoria/"
            f"ingest_date={self.ingest_date}/agg_vendas_uf_categoria.snappy.parquet"
        )


settings = Settings()


def resumo_configuracao() -> str:
    return (
        f"destino={settings.target} | base={settings.base_uri} | "
        f"ingest_date={settings.ingest_date} | seed={settings.seed}"
    )
