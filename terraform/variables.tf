variable "clickhouse_host" {
  description = "ClickHouse host reachable from where Terraform runs."
  type        = string
  default     = "localhost"
}

variable "clickhouse_port" {
  description = "ClickHouse native protocol port."
  type        = number
  default     = 9000
}

variable "clickhouse_protocol" {
  description = "ClickHouse connection protocol."
  type        = string
  default     = "native"

  validation {
    condition     = contains(["native", "nativesecure", "http", "https"], var.clickhouse_protocol)
    error_message = "clickhouse_protocol must be one of: native, nativesecure, http, https."
  }
}

variable "clickhouse_username" {
  description = "ClickHouse username."
  type        = string
  default     = "aeropulse"
}

variable "clickhouse_password" {
  description = "ClickHouse password."
  type        = string
  sensitive   = true
  default     = "aeropulse"
}

variable "schema_names" {
  description = "ClickHouse databases to create for AeroPulse."
  type        = set(string)
  default     = ["aeropulse_raw", "aeropulse_staging", "aeropulse_analytics"]

  validation {
    condition = alltrue([
      for name in var.schema_names : can(regex("^[A-Za-z_][A-Za-z0-9_]*$", name))
    ])
    error_message = "Schema names must be valid ClickHouse identifiers."
  }
}
