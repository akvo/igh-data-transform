# IGH Data Transformation

## Developer Getting Started

### Prerequisites

- [UV](https://docs.astral.sh/uv/) - Fast Python package manager (manages Python versions automatically)

### Installation

1. **Install UV** (if not already installed):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd igh-data-transform
   ```

3. **Install Python and dependencies**:
   ```bash
   # UV will automatically install Python 3.12 if needed
   uv sync
   ```

### Running the Application

You can run the CLI tool without activating the virtual environment using `uv run`:

```bash
# Show available commands
uv run igh-transform --help
```

Alternatively, activate the virtual environment first:

```bash
source .venv/bin/activate  # On Linux/Mac
# or
.venv\Scripts\activate     # On Windows

# Then run normally
igh-transform --help
```

### CLI Commands

#### Bronze to Silver Transformation

Transform raw Bronze layer data to cleaned Silver layer:

```bash
uv run igh-transform bronze-to-silver --bronze-db ./data/bronze.db --silver-db ./data/silver.db
```

This applies cleanup transformations:
- Drops columns that are entirely null (preserves `valid_from`/`valid_to`)
- Normalizes whitespace in text fields
- Ready for table-specific column renames and value mappings

#### Silver to Gold Transformation

Transform Silver layer to pre-aggregated Gold layer (not yet implemented):

```bash
uv run igh-transform silver-to-gold --silver-db ./data/silver.db
```

#### Validate Data Quality

Validate data quality in a database (not yet implemented):

```bash
uv run igh-transform validate --db ./data/silver.db
```

### Pulling Data from Dataverse

This project uses [igh-data-sync](https://github.com/akvo/igh-data-sync) to pull data from Microsoft Dataverse before applying transformations.

**Setup:**

1. **Configure environment variables** - Create a `.env` file with your Dataverse credentials:
   ```bash
   CLIENT_ID=your-azure-client-id
   CLIENT_SECRET=your-azure-client-secret
   SCOPE=https://your-org.crm.dynamics.com/.default
   API_URL=https://your-org.api.crm.dynamics.com/api/data/v9.2/
   SQLITE_DB_PATH=./data/dataverse.db
   ```

2. **Run the sync** - Pull data from Dataverse to local SQLite:
   ```bash
   uv run sync-dataverse
   ```

3. **Verify the data** (optional) - Check foreign key integrity:
   ```bash
   uv run sync-dataverse --verify
   ```

The synced data will be stored in a SQLite database with SCD2 (Slowly Changing Dimension Type 2) versioning for historical tracking.

### Development Workflow

The project uses UV for dependency management. Common commands:

- **Add a dependency**: `uv add <package-name>`
- **Add a dev dependency**: `uv add --dev <package-name>`
- **Update dependencies**: `uv sync`
- **Run commands without activating venv**: `uv run <command>`
- **Run tests**: `uv run pytest`
- **Run linter**: `uv run ruff check src/ tests/`
