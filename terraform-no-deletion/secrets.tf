resource "aws_secretsmanager_secret" "lastupload" {
  name = "lastupload"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "lastupload_version" {
  secret_id     = aws_secretsmanager_secret.lastupload.id
  secret_string = var.lastupload_value
}