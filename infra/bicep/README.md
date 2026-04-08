# Bicep Infrastructure as Code

Azure Bicep templates for deploying the Debezium CDC pipeline.

## Status

This directory is reserved for future Bicep templates to automate:
- Resource group and storage account creation
- Azure Container Registry (ACR)
- Oracle and Debezium container instances
- Azure Files share for persistent state
- Role assignments and RBAC
- Monitoring and diagnostics

## Current Alternative

For now, use the PowerShell deployment script:
```bash
../../scripts/deploy-azure-services.ps1
```

See [DEPLOYMENT_GUIDE.md](../../scripts/DEPLOYMENT_GUIDE.md) for details.

## Roadmap

Planned Bicep templates:
- `main.bicep` - Root template orchestrating all resources
- `modules/storage.bicep` - Storage account and file share
- `modules/acr.bicep` - Azure Container Registry
- `modules/aci.bicep` - Oracle and Debezium container instances
- `modules/rbac.bicep` - Role assignments

## References

- [Bicep Documentation](https://learn.microsoft.com/en-us/azure/azure-resource-manager/bicep/)
- [Bicep Best Practices](https://learn.microsoft.com/en-us/azure/azure-resource-manager/bicep/best-practices)
