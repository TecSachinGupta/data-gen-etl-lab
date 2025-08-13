# Databricks notebook source
ADLS_CONFIG_KEYS = {
    "client_id" : "storage-service-principal-client-id",
    "tenent_id" : "storage-service-principal-tenent-id",
    "secret_value" : "storage-service-principal-secret-value"
}

# COMMAND ----------

EXTRACT_YEAR_REGEX = "(?<=\()(\d+)(?=\))"

# COMMAND ----------

def get_adls_configs(scope_name, config_keys = ADLS_CONFIG_KEYS, storage_account_name = "alldatastore"):
    """
        Retruns the configs for mouting the Azure Storage.
        Arguments:
            scope_name: 
            config_keys:
            storage_account_name: 
        Returns:
            adls_mount_config: 
            adls_spark_config:
    """
    storage_app_client_id = dbutils.secrets.get(scope=scope_name, key=config_keys["client_id"])
    storage_app_tenent_id = dbutils.secrets.get(scope=scope_name, key=config_keys["tenent_id"])
    storage_app_secret_value = dbutils.secrets.get(scope=scope_name, key=config_keys["secret_value"])

    adls_mount_config = {
        f"fs.azure.account.auth.type": "OAuth",
        f"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        f"fs.azure.account.oauth2.client.id": storage_app_client_id,
        f"fs.azure.account.oauth2.client.secret": storage_app_secret_value,
        f"fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{storage_app_tenent_id}/oauth2/token"
    }
    adls_spark_config = { (key + f".{storage_account_name}.dfs.core.windows.net"): adls_mount_config[key] for key in adls_mount_config}
    
    return adls_mount_config, adls_spark_config

# COMMAND ----------



