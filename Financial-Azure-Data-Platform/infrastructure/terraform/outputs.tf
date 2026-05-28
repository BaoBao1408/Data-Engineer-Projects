output "acr_login_server"         { value = azurerm_container_registry.acr.login_server }
output "datalake_account_name"    { value = azurerm_storage_account.datalake.name }
output "datalake_primary_key"     { value = azurerm_storage_account.datalake.primary_access_key; sensitive = true }
output "sql_server_fqdn"          { value = azurerm_mssql_server.edp.fully_qualified_domain_name }
output "cosmos_endpoint"          { value = azurerm_cosmosdb_account.edp.endpoint }
output "cosmos_gremlin_endpoint"  { value = "wss://${azurerm_cosmosdb_account.edp.name}.gremlin.cosmos.azure.com:443/" }
output "cosmos_primary_key"       { value = azurerm_cosmosdb_account.edp.primary_key; sensitive = true }
output "aks_cluster_name"         { value = azurerm_kubernetes_cluster.edp.name }
output "key_vault_uri"            { value = azurerm_key_vault.edp.vault_uri }
output "log_analytics_workspace_id" { value = azurerm_log_analytics_workspace.edp.id }
