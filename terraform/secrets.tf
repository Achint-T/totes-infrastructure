resource "aws_secretsmanager_secret" "database_credentials" {
    name = "database_credentials"
}

resource "aws_secretsmanager_secret_version" "database_credentials_version" {
  secret_id     = aws_secretsmanager_secret.database_credentials.id
  secret_string = var.database_credentials_value
}

resource "aws_secretsmanager_secret" "warehouse_credentials" {
    name = "warehouse_credentials"
}

resource "aws_secretsmanager_secret_version" "warehouse_credentials_version" {
  secret_id     = aws_secretsmanager_secret.warehouse_credentials.id
  secret_string = var.warehouse_credentials_value
}