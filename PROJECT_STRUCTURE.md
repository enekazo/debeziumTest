# Project Structure Overview

Updated directory structure for better organization:

```
Debizium/
├── 📄 README.md              ← Main project documentation
├── 📄 pom.xml                ← Maven configuration
├── LICENSE
├── .gitignore
│
├── 📁 docs/                  ✨ Centralized documentation
│   ├── README.md             ← Start here
│   ├── RUNBOOK.md            ← Local Docker setup
│   ├── RUNBOOK_AZURE_ACI.md  ← Azure deployment
│   └── DEPLOYMENT_GUIDE.md*  ← Deployment script reference (in scripts/)
│
├── 📁 config/                ✨ Configuration templates
│   ├── README.md
│   ├── application.properties.template
│   ├── application-local.properties.example
│   └── application-adls.properties.example
│   └── [application.properties] ← Active config (in src/main/resources/)
│
├── 📁 infra/                 ✨ Infrastructure & deployment
│   ├── docker/
│   │   ├── Dockerfile
│   │   ├── docker-compose.yml
│   │   └── README.md
│   ├── bicep/                (future: Bicep IaC templates)
│   │   └── README.md
│   └── terraform/            (future: Terraform modules)
│       └── README.md
│
├── 📁 scripts/               ✨ Deployment & utility scripts
│   ├── deploy-azure-services.ps1
│   ├── DEPLOYMENT_GUIDE.md
│   ├── utils/                (helper scripts)
│   └── dev/                  (developer tools)
│
├── 📁 sql/                   ✨ Oracle SQL scripts
│   ├── README.md
│   ├── setup/                (initialization)
│   └── validation/           (CDC validation queries)
│
├── 📁 oracle/                ← Oracle container init scripts
│   ├── init/
│   │   ├── 01_enable_supplemental_logging.sh
│   │   ├── 02_create_debezium_user.sh
│   │   ├── 03_create_hr_schema.sql
│   │   └── 04_create_local_pdb_user.sql
│   └── README.md
│
├── 📁 src/                   ← Java source code (Maven standard)
│   ├── main/
│   │   ├── java/io/debezium/server/fabric/
│   │   │   ├── FabricMirroringSink.java
│   │   │   ├── config/
│   │   │   ├── metadata/
│   │   │   ├── parquet/
│   │   │   └── storage/
│   │   └── resources/
│   │       ├── application.properties    ← Active config from config/
│   │       └── META-INF/beans.xml
│   └── test/
│       └── java/...
│
└── 📁 target/                (Maven build output - git ignored)

* DEPLOYMENT_GUIDE.md is in scripts/ for easy reference from command line
  but references should point to docs/ for consistency.
```

## Quick Navigation

### Getting Started
1. **[docs/README.md](docs/README.md)** - Project overview and arch
2. **[docs/RUNBOOK.md](docs/RUNBOOK.md)** - Local Docker Compose
3. **[docs/RUNBOOK_AZURE_ACI.md](docs/RUNBOOK_AZURE_ACI.md)** - Azure deployment

### Configuration
- **[config/README.md](config/README.md)** - All property file options
- **[config/application.properties.template](config/application.properties.template)** - Template with all settings
- **[config/application-local.properties.example](config/application-local.properties.example)** - Local dev example
- **[config/application-adls.properties.example](config/application-adls.properties.example)** - ADLS Gen2 example

### Deployment
- **[scripts/deploy-azure-services.ps1](scripts/deploy-azure-services.ps1)** - Main deployment script
- **[scripts/DEPLOYMENT_GUIDE.md](scripts/DEPLOYMENT_GUIDE.md)** - Script reference
- **[infra/docker/](infra/docker/)** - Docker files and Compose config
- **[infra/bicep/](infra/bicep/)** - (planned) Bicep IaC templates
- **[infra/terraform/](infra/terraform/)** - (planned) Terraform modules

### SQL & Validation
- **[sql/README.md](sql/README.md)** - SQL scripts overview
- **[sql/validation/](sql/validation/)** - CDC validation queries
- **[sql/setup/](sql/setup/)** - Oracle initialization

### Source Code
- **[src/main/java/...](src/main/java/io/debezium/server/fabric/)** - Sink implementation
- **[src/test/java/...](src/test/java/io/debezium/server/fabric/)** - Unit tests

## Key Improvements

✅ **Documentation** - All guides in one place (`docs/`)
✅ **Configuration** - All templates together (`config/`)
✅ **Infrastructure** - Docker, Bicep, Terraform organized (`infra/`)
✅ **SQL Scripts** - Setup and validation separated (`sql/`)
✅ **Scripts** - Deployment scripts with utilities (`scripts/`)
✅ **Source Code** - Maven standard structure (`src/`)

## Building & Deployment

### Local Development
```bash
export ORACLE_PASSWORD="password"
export AZURE_CLIENT_SECRET="secret"
docker compose -f ./infra/docker/docker-compose.yml up
```

### Azure Deployment
```powershell
./scripts/deploy-azure-services.ps1 -CreateInfra -BuildImages -DeployOracle -DeployDebezium
```

### Maven Build
```bash
mvn clean package -DskipTests
```

## Git Considerations

`.gitignore` should contain:
```
target/
*.properties (except templates and examples)
.vscode/
.env
```

Never commit:
- `application.properties` (use template)
- Environment variables with secrets
- `.env` files
