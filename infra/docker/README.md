# Infrastructure & Deployment Configuration

This directory contains all infrastructure-as-code and deployment configurations.

## Structure

- **docker/** - Docker configuration files
  - `Dockerfile` - Debezium application container definition
  - `docker-compose.yml` - Local development environment orchestration

- **bicep/** - Azure Bicep templates (Infrastructure as Code)
  - Templates for automated Azure resource provisioning
  - For production deployments to Azure

- **terraform/** - Terraform configurations (optional IaC alternative)
  - Alternative to Bicep for Terraform users

## Usage

### Local Docker Development
```bash
cd docker
docker compose up -d
```

### Azure Deployment
```bash
cd ../..
./scripts/deploy-azure-services.ps1 -CreateInfra -DeployOracle -DeployDebezium
```

See:
- [docs/RUNBOOK.md](../../docs/RUNBOOK.md) - Local Docker setup
- [docs/RUNBOOK_AZURE_ACI.md](../../docs/RUNBOOK_AZURE_ACI.md) - Azure ACI deployment
- [scripts/DEPLOYMENT_GUIDE.md](../../scripts/DEPLOYMENT_GUIDE.md) - Deployment script reference
