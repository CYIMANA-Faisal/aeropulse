terraform {
  required_version = ">= 1.5.0"

  required_providers {
    clickhousedbops = {
      source  = "ClickHouse/clickhousedbops"
      version = "1.10.0"
    }
  }
}

provider "clickhousedbops" {
  host     = var.clickhouse_host
  port     = var.clickhouse_port
  protocol = var.clickhouse_protocol

  auth_config = {
    strategy = "password"
    username = var.clickhouse_username
    password = var.clickhouse_password
  }
}

resource "clickhousedbops_database" "schemas" {
  for_each = var.schema_names

  name = each.value
}

