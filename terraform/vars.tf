variable "database_credentials_value" {
  description = "Database credentials for the totesys database"
  type        = string
  sensitive   = true
}

variable "warehouse_credentials_value" {
  description = "Database credentials for the final warehouse"
  type        = string
  sensitive   = true
}