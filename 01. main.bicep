// --- Paramètres (Ce que vous changez à chaque déploiement) ---
param environment string = 'dev' // ex: dev, tst, prd
param location string = 'canadacentral'
param appName string = 'data'
param sqlAdminLogin string = 'sqladmin'

@secure()
param sqlAdminPassword string // Sera demandé lors du déploiement

// --- Variables (Calcul automatique) ---
// uniqueString garantit que le nom du stockage est unique mondialement
var uniqueSuffix = uniqueString(resourceGroup().id)
var rgName = 'rg-${appName}-${environment}-cac-001'
var stName = 'st${appName}${environment}${uniqueSuffix}' 
var dbwName = 'dbw-analytics-${environment}-cac-001'
var sqlServerName = 'srv-gizmo-${environment}-${uniqueSuffix}'
var sqlDbName = 'gizmo-box-db'

// --- 1. Storage Account (ADLS Gen2) ---
resource st 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: stName
  location: location
  sku: { name: 'Standard_LRS' } // Pensez au ZRS pour la prod!
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true // CRITIQUE : Active ADLS Gen2
  }
}

// --- 2. Azure Databricks Workspace ---
resource dbw 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name: dbwName
  location: location
  sku: { name: 'premium' } // Requis pour Unity Catalog
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', '${rgName}-dbw-managed')
  }
}

// --- 3. Access Connector for Databricks ---
resource accessConnector 'Microsoft.Databricks/accessConnectors@2023-05-01' = {
  name: 'uami-dbx-access-${environment}-cac'
  location: location
  identity: { type: 'SystemAssigned' }
}

// --- 4. Connexion Storage Account to Databricks ---
resource roleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  scope: st // Le compte de stockage
  name: guid(st.id, accessConnector.id, 'Storage Blob Data Contributor')
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 
    'ba92f5b4-2d11-453d-a403-e96b0029c9fe') // ID fixe du rôle Contributor
    principalId: accessConnector.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

// --- 5. SQL Server & Database ---
resource sqlServer 'Microsoft.Sql/servers@2023-05-01-preview' = {
  name: sqlServerName
  location: location
  properties: {
    administratorLogin: sqlAdminLogin
    administratorLoginPassword: sqlAdminPassword
  }
}

resource sqlDB 'Microsoft.Sql/servers/databases@2023-05-01-preview' = {
  parent: sqlServer
  name: sqlDbName
  location: location
  sku: { name: 'Standard', tier: 'Standard' }
}
