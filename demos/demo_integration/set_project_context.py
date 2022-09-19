# Databricks notebook source
import os

# Read standard environment variable
environment = "dev" 
#os.getenv("ENVIRONMENT")
if environment not in ["dev", "qa", "prod"]:
    raise Exception(
        "This Databricks Workspace does not have necessary environment variables."
        " Contact the admin team to set up the global init script and restart your cluster."
    )

# COMMAND ----------

# Export variables whose values depend on the environment: dev, qa, or prod
if environment == "dev":
    adls_raw_bronze_storage_account_name = "brewdatpltfrmrawbrzd"
    adls_silver_gold_storage_account_name = "brewdatpltfrmslvgldd"
    adls_brewdat_ghq_storage_account_name = "brewdatadlsgbdev"
    adls_raw_bronze_storage_account_name_wiz = "storagepracgen2"
    key_vault_name = "brewdatpltfrmghqtechakvd"
    key_vault_name_wiz = "prac-akv-test-1"
    spn_client_id = "4df9ec8f-0e38-4f07-92f2-6bcbbe5fca5e"
    spn_secret_name = "wiz-spn-pltfrm-ghq-tech-secret-name"
    spn_client_id_wiz = "4df9ec8f-0e38-4f07-92f2-6bcbbe5fca5e"
    spn_secret_name_wiz = "wiz-spn-pltfrm-ghq-tech-secret-name"
    source_system_to_sap_sid = {
        "sap_europe": "ero",
    }
    synapse_blob_storage_account_name = "brewdatpltfrmsynwkssad"
    synapse_connection_string = "jdbc:sqlserver://brewdat-pltfrm-synwks-d.sql.azuresynapse.net:1433;" + \
        "database=poc_sqlpool;encrypt=true;trustServerCertificate=false;loginTimeout=30;" + \
        "hostNameInCertificate=*.sql.azuresynapse.net;Authentication=ActiveDirectoryIntegrated"
elif environment == "qa":
    adls_raw_bronze_storage_account_name = "brewdatpltfrmrawbrzq"
    adls_silver_gold_storage_account_name = "brewdatpltfrmslvgldq"
    adls_brewdat_ghq_storage_account_name = "brewdatadlsgbqa"
    key_vault_name = "brewdatpltfrmghqtechakvq"
    spn_client_id = "12345678-1234-1234-1234-123456789999"
    spn_secret_name = "brewdat-spn-pltfrm-ghq-tech-template-rw-q"
    source_system_to_sap_sid = {
        "sap_europe": "era",
    }
    synapse_blob_storage_account_name = "brewdatpltfrmsynwkssaq"
    synapse_connection_string = "jdbc:sqlserver://brewdat-pltfrm-synwks-q.sql.azuresynapse.net:1433;" + \
        "database=poc_sqlpool;encrypt=true;trustServerCertificate=false;loginTimeout=30;" + \
        "hostNameInCertificate=*.sql.azuresynapse.net;Authentication=ActiveDirectoryIntegrated"
elif environment == "prod":
    adls_raw_bronze_storage_account_name = "brewdatpltfrmrawbrzp"
    adls_silver_gold_storage_account_name = "brewdatpltfrmslvgldp"
    adls_brewdat_ghq_storage_account_name = "brewdatadlsgbprod"
    key_vault_name = "brewdatpltfrmghqtechakvp"
    spn_client_id = "12345678-1234-1234-1234-123456789999"
    spn_secret_name = "brewdat-spn-pltfrm-ghq-tech-template-rw-p"
    source_system_to_sap_sid = {
        "sap_europe": "erp",
    }
    synapse_blob_storage_account_name = "brewdatpltfrmsynwkssap"
    synapse_connection_string = "jdbc:sqlserver://brewdat-pltfrm-synwks-p.sql.azuresynapse.net:1433;" + \
        "database=poc_sqlpool;encrypt=true;trustServerCertificate=false;loginTimeout=30;" + \
        "hostNameInCertificate=*.sql.azuresynapse.net;Authentication=ActiveDirectoryIntegrated"

print(f"adls_raw_bronze_storage_account_name: {adls_raw_bronze_storage_account_name}")
print(f"adls_silver_gold_storage_account_name: {adls_silver_gold_storage_account_name}")
print(f"adls_brewdat_ghq_storage_account_name: {adls_brewdat_ghq_storage_account_name}")
print(f"adls_raw_bronze_storage_account_name_maz: {adls_raw_bronze_storage_account_name_maz}")
print(f"key_vault_name: {key_vault_name}")
print(f"spn_client_id: {spn_client_id}")
print(f"spn_secret_name: {spn_secret_name}")
print(f"key_vault_name_maz: {key_vault_name_maz}")
print(f"spn_client_id_maz: {spn_client_id_maz}")
print(f"spn_secret_name_maz: {spn_secret_name_maz}")
print(f"source_system_to_sap_sid: {source_system_to_sap_sid}")
print(f"synapse_blob_storage_account_name: {synapse_blob_storage_account_name}")
print(f"synapse_connection_string: {synapse_connection_string}")

# COMMAND ----------

# Export additional helper variables
synapse_blob_temp_root = f"abfss://temp-csa@{synapse_blob_storage_account_name}.dfs.core.windows.net"
print(f"synapse_blob_temp_root: {synapse_blob_temp_root}")

lakehouse_raw_root = f"abfss://raw@{adls_raw_bronze_storage_account_name}.dfs.core.windows.net"
print(f"lakehouse_raw_root: {lakehouse_raw_root}")

lakehouse_bronze_root = f"abfss://bronze@{adls_raw_bronze_storage_account_name}.dfs.core.windows.net"
print(f"lakehouse_bronze_root: {lakehouse_bronze_root}")

lakehouse_silver_root = f"abfss://silver@{adls_silver_gold_storage_account_name}.dfs.core.windows.net"
print(f"lakehouse_silver_root: {lakehouse_silver_root}")

brewdat_ghq_root = f"abfss://brewdat-ghq@{adls_brewdat_ghq_storage_account_name}.dfs.core.windows.net"
print(f"brewdat_ghq_root: {brewdat_ghq_root}")

lakehouse_raw_root_wiz = f"abfss://raw@{adls_raw_bronze_storage_account_name_wiz}.dfs.core.windows.net"
print(f"lakehouse_raw_root_wiz: {lakehouse_raw_root_wiz}")

lakehouse_bronze_root_wiz = f"abfss://bronze@{adls_raw_bronze_storage_account_name_wiz}.dfs.core.windows.net"
print(f"lakehouse_bronze_root_wiz: {lakehouse_bronze_root_wiz}")

lakehouse_silver_root_wiz = f"abfss://silver@{adls_raw_bronze_storage_account_name_wiz}.dfs.core.windows.net"
print(f"lakehouse_silver_root_wiz: {lakehouse_silver_root_wiz}")
