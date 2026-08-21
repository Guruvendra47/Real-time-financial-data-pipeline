# dbt Command Reference

This README is a practical command guide for dbt Core with Snowflake, Databricks, and Redshift.

## 1. What dbt does

dbt is a command-line tool for transforming data already loaded in your warehouse or lakehouse. It helps you build models, test data, document the project, and run transformations in a repeatable way.

## 2. Install dbt Core

### Recommended install pattern

```bash
python -m pip install dbt-core
```

### Install adapter plugins

Install the adapter that matches your platform:

```bash
python -m pip install dbt-snowflake
python -m pip install dbt-databricks
python -m pip install dbt-redshift
```

Notes:

* In modern dbt versions, adapters are installed separately.
* Installing an adapter also brings in dbt Core and required dependencies for that adapter.

## 3. Create a virtual environment

### Windows PowerShell

```powershell
python -m venv dbt-env
.\dbt-env\Scripts\Activate.ps1
python -m pip install --upgrade pip
```

### macOS / Linux

```bash
python3 -m venv dbt-env
source dbt-env/bin/activate
python -m pip install --upgrade pip
```

## 4. Start a dbt project

```bash
dbt init snowflake_project
cd snowflake_project
```

If the adapter is installed, dbt will show the matching platform during project setup.

## 5. Configure the connection

dbt uses a `profiles.yml` file for credentials and warehouse connection details.

Typical location:

* Windows: `C:\Users\<your-user>\.dbt\profiles.yml`
* macOS/Linux: `~/.dbt/profiles.yml`

### Snowflake example

```yaml
snowflake_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your_account_identifier
      user: your_username
      password: your_password
      role: your_role
      database: your_database
      warehouse: your_warehouse
      schema: public
      threads: 4
```

### Databricks example

```yaml
dbt_project:
  target: dev
  outputs:
    dev:
      type: databricks
      host: your_workspace_host
      http_path: your_http_path
      token: your_personal_access_token
      catalog: your_catalog
      schema: your_schema
      threads: 4
```

### Redshift example

```yaml
dbt_project:
  target: dev
  outputs:
    dev:
      type: redshift
      host: your_cluster_endpoint
      user: your_username
      password: your_password
      port: 5439
      dbname: your_database
      schema: public
      threads: 4
```

## 6. Most used dbt terminal commands

### Project and connection

```bash
dbt init <project_name>
dbt debug
dbt deps
dbt clean
```

### Build and transformation

```bash
dbt run
dbt build
dbt compile
dbt seed
dbt snapshot
```

### Testing and quality checks

```bash
dbt test
dbt source freshness
```

### Documentation

```bash
dbt docs generate
dbt docs serve
```

## 7. What each command does

* `dbt debug` checks your connection and project setup.
* `dbt deps` installs package dependencies listed in `packages.yml`.
* `dbt clean` removes generated files and package artifacts.
* `dbt run` builds models only.
* `dbt build` runs models, tests, seeds, snapshots, and UDFs in dependency order.
* `dbt compile` generates executable SQL without materializing models.
* `dbt seed` loads CSV files from the `seeds/` directory.
* `dbt snapshot` captures slowly changing data over time.
* `dbt test` runs data tests and unit tests.
* `dbt source freshness` checks how fresh your source data is.
* `dbt docs generate` builds project documentation.
* `dbt docs serve` opens the documentation site locally.

## 8. Common model commands

### Run a single model

```bash
dbt run --select model_name
```

### Run multiple models

```bash
dbt run --select model1 model2
```

### Run a folder

```bash
dbt run --select path:models/staging
```

### Run a model and its downstream dependencies

```bash
dbt run --select model_name+
```

### Run a model and its parents

```bash
dbt run --select +model_name
```

### Test a single model

```bash
dbt test --select model_name
```

## 9. Useful select patterns

```bash
dbt run --select tag:staging
dbt run --select config.materialized:table
dbt test --select resource_type:model
```

## 10. Package management

### Add a package

Edit `packages.yml`:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```

Then install:

```bash
dbt deps
```

## 11. Adapter install commands

### Snowflake

```bash
python -m pip install dbt-snowflake
```

### Databricks

```bash
python -m pip install dbt-databricks
```

### Redshift

```bash
python -m pip install dbt-redshift
```

## 12. Example workflow

```bash
python -m venv dbt-env
.\dbt-env\Scripts\Activate.ps1
python -m pip install dbt-snowflake

dbt init snowflake_project
cd snowflake_project
dbt debug
dbt deps
dbt run
dbt test
dbt docs generate
dbt docs serve
```

## 13. Troubleshooting

### Adapter not showing during `dbt init`

Install the adapter plugin first, then rerun `dbt init`.

### Connection fails

Check:

* account / host / endpoint
* username or token
* password or private key
* warehouse / cluster / http path
* database / catalog / schema

### Project looks broken

Try:

```bash
dbt clean
dbt deps
dbt debug
```

## 14. Quick reference

```bash
dbt --version
dbt init
dbt debug
dbt deps
dbt clean
dbt run
dbt build
dbt compile
dbt seed
dbt snapshot
dbt test
dbt source freshness
dbt docs generate
dbt docs serve
```

## 15. Suggested next step

Use this README alongside your project files so the same command set can be reused for Snowflake, Databricks, and Redshift projects.
