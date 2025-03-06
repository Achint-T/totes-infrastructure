variable "lastupload_value" {
  description = "The last upload timestamp"
  type        = string
  default     = "2020-01-01 00:00:00"
}

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