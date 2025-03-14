resource "aws_secretsmanager_secret" "lastupload" {
  name = "lastupload"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "lastupload_version" {
  secret_id     = aws_secretsmanager_secret.lastupload.id
  secret_string = var.lastupload_value
}

resource "aws_secretsmanager_secret" "database_credentials" {
    name = "database_credentials"
    recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "database_credentials_version" {
  secret_id     = aws_secretsmanager_secret.database_credentials.id
  secret_string = var.database_credentials_value
}

resource "aws_secretsmanager_secret" "warehouse_credentials" {
    name = "warehouse_credentials"
    recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "warehouse_credentials_version" {
  secret_id     = aws_secretsmanager_secret.warehouse_credentials.id
  secret_string = var.warehouse_credentials_value
}