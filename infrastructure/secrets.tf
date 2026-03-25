# -----------------------------------------------------------------------------
# Secrets Manager — Database credentials for DB2 and MSSQL Server
# -----------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "db2" {
  count       = var.enable_db2 ? 1 : 0
  name        = "${local.project_prefix}/db2-credentials"
  description = "DB2 JDBC connection credentials for SAS-migrated jobs"

  tags = {
    DataSource = "DB2"
  }
}

resource "aws_secretsmanager_secret_version" "db2" {
  count     = var.enable_db2 ? 1 : 0
  secret_id = aws_secretsmanager_secret.db2[0].id

  # Populate via CLI or console — placeholder structure
  secret_string = jsonencode({
    engine   = "db2"
    host     = var.db2_host
    port     = var.db2_port
    database = var.db2_database
    username = var.db2_username
    password = var.db2_password
  })

  lifecycle {
    ignore_changes = [secret_string]
  }
}

resource "aws_secretsmanager_secret" "mssql" {
  count       = var.enable_mssql ? 1 : 0
  name        = "${local.project_prefix}/mssql-credentials"
  description = "MSSQL Server JDBC connection credentials for SAS-migrated jobs"

  tags = {
    DataSource = "MSSQL"
  }
}

resource "aws_secretsmanager_secret_version" "mssql" {
  count     = var.enable_mssql ? 1 : 0
  secret_id = aws_secretsmanager_secret.mssql[0].id

  secret_string = jsonencode({
    engine   = "mssql"
    host     = var.mssql_host
    port     = var.mssql_port
    database = var.mssql_database
    username = var.mssql_username
    password = var.mssql_password
  })

  lifecycle {
    ignore_changes = [secret_string]
  }
}
