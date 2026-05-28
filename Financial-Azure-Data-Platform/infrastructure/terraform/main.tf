# ══════════════════════════════════════════════════════════════════════════════
# Terraform – Azure Infrastructure for Enterprise Data Platform
# Resources: AKS, Azure SQL, ADLS Gen2, ACR, Cosmos DB, Key Vault, Log Analytics
# ══════════════════════════════════════════════════════════════════════════════

terraform {
  required_version = ">= 1.7.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.50"
    }
  }
  backend "azurerm" {
    resource_group_name  = "rg-edp-tfstate"
    storage_account_name = "edptfstate"
    container_name       = "tfstate"
    key                  = "edp.terraform.tfstate"
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = false
    }
  }
}

# ─── Data Sources ─────────────────────────────────────────────────────────────
data "azurerm_client_config" "current" {}

# ─── Resource Group ───────────────────────────────────────────────────────────
resource "azurerm_resource_group" "edp" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.common_tags
}

# ─── Azure Container Registry ────────────────────────────────────────────────
resource "azurerm_container_registry" "acr" {
  name                = "${var.project_prefix}registry"
  resource_group_name = azurerm_resource_group.edp.name
  location            = azurerm_resource_group.edp.location
  sku                 = "Standard"
  admin_enabled       = false
  tags                = var.common_tags
}

# ─── Azure Key Vault ──────────────────────────────────────────────────────────
resource "azurerm_key_vault" "edp" {
  name                       = "${var.project_prefix}-kv"
  resource_group_name        = azurerm_resource_group.edp.name
  location                   = azurerm_resource_group.edp.location
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  purge_protection_enabled   = true
  soft_delete_retention_days = 90
  tags                       = var.common_tags

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id
    secret_permissions = ["Get", "List", "Set", "Delete", "Purge"]
  }
}

# Store DB password in Key Vault
resource "azurerm_key_vault_secret" "sql_password" {
  name         = "azure-sql-password"
  value        = var.sql_admin_password
  key_vault_id = azurerm_key_vault.edp.id
}

# ─── Azure Data Lake Storage Gen2 ─────────────────────────────────────────────
resource "azurerm_storage_account" "datalake" {
  name                     = "${var.project_prefix}datalake"
  resource_group_name      = azurerm_resource_group.edp.name
  location                 = azurerm_resource_group.edp.location
  account_tier             = "Standard"
  account_replication_type = "GRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true   # Hierarchical Namespace = ADLS Gen2
  min_tls_version          = "TLS1_2"
  tags                     = var.common_tags

  blob_properties {
    versioning_enabled = true
    delete_retention_policy {
      days = 30
    }
  }

  network_rules {
    default_action = "Allow"   # Restrict in production to VNet
  }
}

# Medallion zones
resource "azurerm_storage_container" "raw" {
  name                  = "raw"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}
resource "azurerm_storage_container" "processed" {
  name                  = "processed"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}
resource "azurerm_storage_container" "curated" {
  name                  = "curated"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

# ─── Azure SQL Database ───────────────────────────────────────────────────────
resource "azurerm_mssql_server" "edp" {
  name                         = "${var.project_prefix}-sql-server"
  resource_group_name          = azurerm_resource_group.edp.name
  location                     = azurerm_resource_group.edp.location
  version                      = "12.0"
  administrator_login          = var.sql_admin_username
  administrator_login_password = var.sql_admin_password
  minimum_tls_version          = "1.2"
  tags                         = var.common_tags

  azuread_administrator {
    login_username = var.aad_admin_username
    object_id      = var.aad_admin_object_id
  }
}

resource "azurerm_mssql_database" "edp" {
  name           = "edp-warehouse"
  server_id      = azurerm_mssql_server.edp.id
  collation      = "SQL_Latin1_General_CP1_CI_AS"
  sku_name       = var.sql_sku          # "S3" for staging, "P2" for production
  max_size_gb    = var.sql_max_size_gb
  zone_redundant = var.environment == "production"
  tags           = var.common_tags

  short_term_retention_policy {
    retention_days = 35
  }
}

resource "azurerm_mssql_firewall_rule" "azure_services" {
  name             = "AllowAzureServices"
  server_id        = azurerm_mssql_server.edp.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

# ─── Azure Cosmos DB (Gremlin API) – Knowledge Graph ─────────────────────────
resource "azurerm_cosmosdb_account" "edp" {
  name                = "${var.project_prefix}-cosmos"
  resource_group_name = azurerm_resource_group.edp.name
  location            = azurerm_resource_group.edp.location
  offer_type          = "Standard"
  kind                = "GlobalDocumentDB"
  tags                = var.common_tags

  capabilities {
    name = "EnableGremlin"
  }

  consistency_policy {
    consistency_level       = "Session"
    max_interval_in_seconds = 5
    max_staleness_prefix    = 100
  }

  geo_location {
    location          = azurerm_resource_group.edp.location
    failover_priority = 0
  }

  backup {
    type               = "Periodic"
    interval_in_minutes = 240
    retention_in_hours  = 8
  }
}

resource "azurerm_cosmosdb_gremlin_database" "edp" {
  name                = "edp-graph"
  resource_group_name = azurerm_resource_group.edp.name
  account_name        = azurerm_cosmosdb_account.edp.name
}

resource "azurerm_cosmosdb_gremlin_graph" "entities" {
  name                = "entities"
  resource_group_name = azurerm_resource_group.edp.name
  account_name        = azurerm_cosmosdb_account.edp.name
  database_name       = azurerm_cosmosdb_gremlin_database.edp.name
  partition_key_path  = "/entity_type"
  throughput          = 400

  index_policy {
    automatic      = true
    indexing_mode  = "consistent"
    included_paths = ["/*"]
    excluded_paths = ["/\"_etag\"/?"]
  }
}

# ─── Log Analytics Workspace ──────────────────────────────────────────────────
resource "azurerm_log_analytics_workspace" "edp" {
  name                = "${var.project_prefix}-logs"
  resource_group_name = azurerm_resource_group.edp.name
  location            = azurerm_resource_group.edp.location
  sku                 = "PerGB2018"
  retention_in_days   = 90
  tags                = var.common_tags
}

# ─── AKS Cluster ─────────────────────────────────────────────────────────────
resource "azurerm_kubernetes_cluster" "edp" {
  name                = "${var.project_prefix}-aks"
  resource_group_name = azurerm_resource_group.edp.name
  location            = azurerm_resource_group.edp.location
  dns_prefix          = "${var.project_prefix}-aks"
  kubernetes_version  = var.aks_kubernetes_version
  tags                = var.common_tags

  default_node_pool {
    name                = "system"
    node_count          = 2
    vm_size             = "Standard_D2s_v3"
    os_disk_size_gb     = 128
    type                = "VirtualMachineScaleSets"
    enable_auto_scaling = true
    min_count           = 2
    max_count           = 5
  }

  identity {
    type = "SystemAssigned"
  }

  oms_agent {
    log_analytics_workspace_id = azurerm_log_analytics_workspace.edp.id
  }

  network_profile {
    network_plugin    = "azure"
    load_balancer_sku = "standard"
  }
}

# AKS pull permission from ACR
resource "azurerm_role_assignment" "aks_acr_pull" {
  principal_id                     = azurerm_kubernetes_cluster.edp.kubelet_identity[0].object_id
  role_definition_name             = "AcrPull"
  scope                            = azurerm_container_registry.acr.id
  skip_service_principal_aad_check = true
}

# AKS read from Key Vault
resource "azurerm_role_assignment" "aks_kv_read" {
  principal_id         = azurerm_kubernetes_cluster.edp.identity[0].principal_id
  role_definition_name = "Key Vault Secrets User"
  scope                = azurerm_key_vault.edp.id
}

# AKS read/write ADLS
resource "azurerm_role_assignment" "aks_storage_contributor" {
  principal_id         = azurerm_kubernetes_cluster.edp.identity[0].principal_id
  role_definition_name = "Storage Blob Data Contributor"
  scope                = azurerm_storage_account.datalake.id
}
