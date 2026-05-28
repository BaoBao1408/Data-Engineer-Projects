variable "project_prefix"       { default = "edp" }
variable "location"             { default = "southeastasia" }   # Singapore (closest to VN)
variable "resource_group_name"  { default = "rg-edp-prod" }
variable "environment"          { default = "staging" }

variable "sql_admin_username"   { default = "edp_admin" }
variable "sql_admin_password"   { sensitive = true }
variable "sql_sku"              { default = "S3" }
variable "sql_max_size_gb"      { default = 50 }

variable "aad_admin_username"   { default = "edp-dba@kpmg.com.vn" }
variable "aad_admin_object_id"  {}

variable "aks_kubernetes_version" { default = "1.29" }

variable "common_tags" {
  default = {
    Project     = "EnterpriseDataPlatform"
    ManagedBy   = "Terraform"
    Team        = "DataEngineering"
    CostCenter  = "KPMG-Innovation"
  }
}
