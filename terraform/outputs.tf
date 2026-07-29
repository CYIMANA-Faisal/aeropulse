output "schema_names" {
  description = "ClickHouse schemas managed by Terraform."
  value       = sort(tolist(var.schema_names))
}

