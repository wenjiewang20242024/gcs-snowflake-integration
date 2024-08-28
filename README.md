# GCS to Snowflake Data Integration

This project is designed to automate the process of loading data from Google Cloud Storage (GCS) to Snowflake.

## Directory Structure
- `.github/workflows/`: GitHub Actions workflows.
- `Customer/`: Contains the configuration files for customer data loading.
- `Loading_yaml/`: Contains additional YAML files for loading data.
- `Sales/`: Placeholder for sales-related data and configurations.
- `Transaction/`: Placeholder for transaction-related data and configurations.
- `file_yaml.yml`: Configuration for general data loading.
- `Process.yml`: Defines the steps in the data load process.
- `testing.yml`: Test scenarios for data loading.

## How to Use

1. **Clone the repository**: 
    ```bash
    git clone https://github.com/your-username/gcs-snowflake-integration.git
    cd gcs-snowflake-integration
    ```

2. **Configure Google Cloud and Snowflake**:
   Ensure that the required GCP service account and Snowflake credentials are set up.

3. **Push changes to trigger GitHub Actions**:
   Any changes pushed to the `main` branch will trigger the GitHub Actions workflow to deploy and execute the data load functions.

4. **Verify the data load**:
   Check Snowflake for the loaded data and verify against the test scenarios in `testing.yml`.
