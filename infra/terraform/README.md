# Terraform Infrastructure as Code

Terraform configurations for deploying the Debezium CDC pipeline.

## Status

This directory is reserved for future Terraform templates as an alternative to Bicep.

Terraform offers:
- State management and drift detection
- Provider ecosystem flexibility
- HCL configuration language
- Module reusability

## Current Alternative

For now, use the PowerShell deployment script:
```bash
../../scripts/deploy-azure-services.ps1
```

Or Azure Bicep in `../bicep/`.

## Planned Structure

```
terraform/
├── main.tf               # Root configuration
├── variables.tf          # Input variables
├── outputs.tf            # Output values
├── terraform.tfvars      # Variable values (not committed)
├── modules/
│   ├── storage/          # Storage account and file share
│   ├── acr/              # Azure Container Registry
│   ├── aci/              # Oracle and Debezium instances
│   └── rbac/             # Role assignments
└── environments/
    ├── dev.tfvars
    ├── staging.tfvars
    └── prod.tfvars
```

## References

- [Terraform on Azure](https://learn.microsoft.com/en-us/azure/developer/terraform/)
- [Terraform Best Practices](https://www.terraform.io/cloud-docs/best-practices)
