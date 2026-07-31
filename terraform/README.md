# AeroPulse Terraform

This Terraform root manages the ClickHouse databases used as AeroPulse schemas:

- `aeropulse_raw`
- `aeropulse_staging`
- `aeropulse_analytics`

## Usage

Start the local stack first:

```bash
docker compose up -d
```

Then run Terraform:

```bash
cd terraform
terraform init
terraform apply
```

The defaults connect to the local ClickHouse container through the native protocol on `localhost:9000` with username/password `aeropulse`/`aeropulse`.

If a database already exists before Terraform manages it, import it first:

```bash
terraform import 'clickhousedbops_database.schemas["aeropulse_raw"]' aeropulse_raw
terraform import 'clickhousedbops_database.schemas["aeropulse_staging"]' aeropulse_staging
terraform import 'clickhousedbops_database.schemas["aeropulse_analytics"]' aeropulse_analytics
```
