# PysparkInDatabricks
Sample code repository using Pyspark environment in Azure Databricks

### Configure the Azure Data Storage with Databricks using Service Principal and Key Vault

#### Step 1: Setup ADLS
1. Go to Azure Portal
2. Search for Storage Account
3. Create new storage Account by clicking create button
4. Provide the Basic details required and click "Next: Advance" button
5. Check the "Enable hierarchical namespace" 
6. Review other setting 
7. Create the storage account

#### Step 2: Setup Service Principal

1. Search for "Microsoft Entra ID"(new name) or "Azure Active Directory"(old)
2. Click on App registration
3. Click new registration
4. Enter name and set the other configs or leave default for learning prupose.
5. click on register button

#### Step 3: Get the required Credentials
1. Go to the App ceated or registered
2. Copy the values for "Application (client) ID" and "Directory (tenant) ID"
3. Go to ceritificates and secrets sectio for the app
4. Select client secrets
5. click on New client secret button
6. Enter description and select the expiration time 
7. click the Add button
8. copy the secret value 

#### Step 4: Add the principal to ADLS
1. Go the Storage Accout or the created ADLS
2. Go to Access Cotrol(IAM) and click Add button and select Role assignment
3. Search for "Storage Account Contributor" and click next
4. Click on Select member and search for the App created
5. Select the App and click select button
6. Click Next and then Click Review and Assign button

#### Step 5: Setup Key Vault
1. Search for Key vault
2. Create a new vault if not exists
3. Change Permission model to "vault access policy" under Access Configuration
4. Go to Properties of the Key vault i.e. Newly created or existing one
5. Copy the "Vault URI" and "Resource ID"

#### Step 6: Setup Azure Managed Secret Scope
1. Go to <databricks>#secrets/createScope (replace the <databricks> with the databricks url)
2. Enter the name for Secret scope
3. Enter the "Vault URI" in the "DNS Name" field
4. Enter the "Resource ID" in the "Resource ID" field
5. Click on create button and Note down the name of secret scope.

#### Create Secrets in Azure Key Vault
1. Go to the Key vault
2. Go the Secrets
3. Click on generate/import button
4. Enter the name and Secret value
5. Optional: Check the Activation and Expiration date checkbox and select the dates
6. Click on create button