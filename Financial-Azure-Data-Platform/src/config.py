"""
Application configuration using Pydantic Settings.
Loads from environment variables and .env file.
"""
from functools import lru_cache
from typing import Literal, Optional

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="WAREHOUSE_DB_")

    user: str = "edp_user"
    password: str = "edp_pass"
    name: str = "edp_warehouse"
    host: str = "postgres-warehouse"
    port: int = 5432

    @computed_field
    @property
    def url(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

    @computed_field
    @property
    def async_url(self) -> str:
        return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


class AzureSQLSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AZURE_SQL_")

    server: str = ""
    database: str = ""
    user: str = ""
    password: str = ""

    @computed_field
    @property
    def connection_string(self) -> str:
        return (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server={self.server};"
            f"Database={self.database};"
            f"UID={self.user};"
            f"PWD={self.password};"
            f"Encrypt=yes;TrustServerCertificate=no;"
        )


class Neo4jSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="NEO4J_")

    uri: str = "bolt://neo4j:7687"
    user: str = "neo4j"
    password: str = "neo4j_pass"
    max_connection_pool_size: int = 50


class CosmosGremlinSettings(BaseSettings):
    """Azure Cosmos DB Gremlin API (production Knowledge Graph)"""
    model_config = SettingsConfigDict(env_prefix="COSMOS_")

    gremlin_endpoint: str = ""
    gremlin_key: str = ""
    database: str = "edp-graph"
    graph: str = "entities"


class StorageSettings(BaseSettings):
    """Azure Data Lake Storage Gen2 / MinIO (local)"""
    # Azure
    azure_storage_account_name: str = ""
    azure_storage_account_key: str = ""
    azure_storage_connection_string: str = ""
    adls_container_raw: str = "raw"
    adls_container_processed: str = "processed"
    adls_container_curated: str = "curated"

    # MinIO (local)
    minio_endpoint: str = "minio:9000"
    minio_access_key: str = "minio_admin"
    minio_secret_key: str = "minio_secret"
    minio_secure: bool = False


class AzureIdentitySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AZURE_")

    client_id: str = ""
    client_secret: str = ""
    tenant_id: str = ""
    subscription_id: str = ""
    key_vault_url: str = ""


class VectorStoreSettings(BaseSettings):
    chroma_host: str = "chromadb"
    chroma_port: int = 8000
    chroma_collection_documents: str = "documents"
    chroma_collection_knowledge: str = "knowledge"


class OpenAISettings(BaseSettings):
    openai_api_key: str = ""
    azure_openai_endpoint: str = ""
    azure_openai_api_key: str = ""
    azure_openai_api_version: str = "2024-02-01"
    embedding_model: str = "text-embedding-ada-002"
    llm_model: str = "gpt-4o"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # App
    env: Literal["development", "staging", "production"] = "development"
    log_level: str = "INFO"
    secret_key: str = "change-me"
    api_key: str = "dev-api-key"
    redis_url: str = "redis://redis:6379/0"

    # Sub-configs
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    azure_sql: AzureSQLSettings = Field(default_factory=AzureSQLSettings)
    neo4j: Neo4jSettings = Field(default_factory=Neo4jSettings)
    cosmos: CosmosGremlinSettings = Field(default_factory=CosmosGremlinSettings)
    storage: StorageSettings = Field(default_factory=StorageSettings)
    azure_identity: AzureIdentitySettings = Field(default_factory=AzureIdentitySettings)
    vector_store: VectorStoreSettings = Field(default_factory=VectorStoreSettings)
    openai: OpenAISettings = Field(default_factory=OpenAISettings)

    @property
    def is_production(self) -> bool:
        return self.env == "production"

    @property
    def is_development(self) -> bool:
        return self.env == "development"


@lru_cache()
def get_settings() -> Settings:
    """Cached settings instance."""
    return Settings()
