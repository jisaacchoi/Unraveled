# Security Guide: Handling Passwords and Sensitive Data

## Current Issue

The `config.yaml` file currently contains database passwords in plain text, which is a security risk:
- Passwords are visible in version control
- Passwords are visible to anyone with access to the config file
- Passwords cannot be easily rotated without changing the config file

## Recommended Solutions

### Option 1: Environment Variables (Recommended for Development)

**Best for:** Local development, single-user deployments

#### Implementation

1. **Remove password from `config.yaml`:**
```yaml
database:
  connection_string: ""  # Leave empty to use individual components
  host: "localhost"
  port: 5432
  database: "postgres"
  username: "postgres"
  password: ""  # Leave empty - will be read from environment variable
  sslmode: ""
```

2. **Set environment variable:**
```bash
# Windows (PowerShell)
$env:DB_PASSWORD="your_password_here"

# Windows (CMD)
set DB_PASSWORD=your_password_here

# Linux/Mac
export DB_PASSWORD="your_password_here"
```

3. **Update `src/shared/database.py` to read from environment:**
```python
def build_connection_string(config: Dict[str, Any]) -> str:
    """Build PostgreSQL connection string from config."""
    db_config = config.get("database", {})
    
    # Check for connection string first
    connection_string = db_config.get("connection_string", "").strip()
    if connection_string:
        return connection_string
    
    # Build from components
    host = db_config.get("host", "localhost")
    port = db_config.get("port", 5432)
    database = db_config.get("database", "postgres")
    username = db_config.get("username", "postgres")
    
    # Read password from environment variable if not in config
    password = db_config.get("password", "").strip()
    if not password:
        password = os.environ.get("DB_PASSWORD", "")
        if not password:
            raise ValueError(
                "Database password not found in config or DB_PASSWORD environment variable. "
                "Please set DB_PASSWORD environment variable or add password to config."
            )
    
    sslmode = db_config.get("sslmode", "")
    
    # Build connection string
    conn_str = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    if sslmode:
        conn_str += f"?sslmode={sslmode}"
    
    return conn_str
```

4. **Create `.env.example` file (template):**
```bash
# Database Configuration
DB_PASSWORD=your_password_here

# Optional: Other sensitive values
# API_KEY=your_api_key_here
```

5. **Add to `.gitignore`:**
```
.env
*.env
config.local.yaml
```

### Option 2: Separate Config File (Recommended for Production)

**Best for:** Production deployments, multiple environments

#### Implementation

1. **Create `config.prod.yaml` (not in git):**
```yaml
database:
  password: "production_password_here"
```

2. **Keep `config.yaml` as template:**
```yaml
database:
  password: ""  # Set in config.prod.yaml or environment variable
```

3. **Load config with override:**
```python
# In src/shared/config.py
def load_config(base_path: Path, override_path: Optional[Path] = None) -> Dict[str, Any]:
    """Load config with optional override file."""
    config = yaml.safe_load(base_path.read_text(encoding="utf-8"))
    
    # Load override file if specified
    if override_path and override_path.exists():
        override = yaml.safe_load(override_path.read_text(encoding="utf-8"))
        # Deep merge override into base config
        config = _deep_merge(config, override)
    
    # ... rest of config loading ...
    return config
```

4. **Usage:**
```bash
# Development (uses config.yaml)
python commands/02_ingest.py --config config.yaml

# Production (uses config.yaml + config.prod.yaml override)
python commands/02_ingest.py --config config.yaml --override config.prod.yaml
```

### Option 3: Secret Management Service (Recommended for Enterprise)

**Best for:** Large organizations, cloud deployments, compliance requirements

#### AWS Secrets Manager
```python
import boto3
import json

def get_secret(secret_name: str) -> Dict[str, Any]:
    """Get secret from AWS Secrets Manager."""
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

# In database.py
secret = get_secret("mrf-pipeline/database")
password = secret["password"]
```

#### Azure Key Vault
```python
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

def get_secret(secret_name: str) -> str:
    """Get secret from Azure Key Vault."""
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url="https://your-vault.vault.azure.net/", credential=credential)
    return client.get_secret(secret_name).value
```

#### HashiCorp Vault
```python
import hvac

def get_secret(secret_path: str) -> Dict[str, Any]:
    """Get secret from HashiCorp Vault."""
    client = hvac.Client(url="https://vault.example.com")
    client.token = os.environ.get("VAULT_TOKEN")
    return client.secrets.kv.v2.read_secret_version(path=secret_path)["data"]["data"]
```

## Migration Steps

### Step 1: Update Code
1. Modify `src/shared/database.py` to support environment variables
2. Update `src/shared/config.py` if using config overrides

### Step 2: Update Config
1. Remove password from `config.yaml`
2. Set environment variable or create override file

### Step 3: Test
1. Verify connection works with environment variable
2. Ensure config file no longer contains password

### Step 4: Update Documentation
1. Update README with password handling instructions
2. Create `.env.example` template
3. Update deployment documentation

## Best Practices

1. **Never commit passwords to version control**
   - Use `.gitignore` to exclude sensitive files
   - Use `git-secrets` to prevent accidental commits

2. **Use different passwords for different environments**
   - Development, staging, production should have separate credentials

3. **Rotate passwords regularly**
   - Change passwords periodically
   - Update environment variables or secret stores

4. **Use least privilege**
   - Database user should only have necessary permissions
   - Don't use superuser accounts for application connections

5. **Monitor access**
   - Log database connection attempts
   - Alert on failed authentication attempts

6. **Use connection encryption**
   - Always use SSL/TLS for database connections in production
   - Set `sslmode: require` in production configs

## Example: Complete Implementation

See `src/shared/database.py` for a complete implementation that:
- Checks for `connection_string` first
- Falls back to individual components
- Reads password from `DB_PASSWORD` environment variable if not in config
- Provides clear error messages if password is missing

## Additional Security Considerations

1. **API Keys**: If you add API integrations, use the same pattern for API keys
2. **File Permissions**: Ensure config files have restricted permissions (chmod 600)
3. **Audit Logging**: Log when sensitive operations occur
4. **Network Security**: Use VPNs or private networks for database access
5. **Encryption at Rest**: Encrypt database backups and log files
