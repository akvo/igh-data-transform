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
# Run the CLI directly
uv run igh-data-transform

# Or run Python scripts directly
uv run python -m igh_data_transform
```

Alternatively, activate the virtual environment first:

```bash
source .venv/bin/activate  # On Linux/Mac
# or
.venv\Scripts\activate     # On Windows

# Then run normally
igh-data-transform
```

### Development Workflow

The project uses UV for dependency management. Common commands:

- **Add a dependency**: `uv add <package-name>`
- **Add a dev dependency**: `uv add --dev <package-name>`
- **Update dependencies**: `uv sync`
- **Run commands without activating venv**: `uv run <command>`
- **Run Python scripts**: `uv run python <script.py>`

### Data access

The project currently uses local dataverse currently. In the long term this will be updated to a python library that 
contains the data. For the time being the data can be accessed here: https://drive.google.com/file/d/1QMef9z_TlusQR1iYADFsKdL8UGgZVrrI/view?usp=drive_link
