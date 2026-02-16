@description('Location of all resources')
param location string = resourceGroup().location

@description('Naming prefix (ex: analytics)')
param namePrefix string = 'analytics'

@description('Environment name (ex: dev, test, prod)')
param env string = 'dev'

@description('Deterministic suffix for uniqueness')
param suffix string = uniqueString(resourceGroup().id)

@description('Log Analytics daily retention (days)')
param logRetentionInDays int = 30

@description('Enable Log Analytics Workspace Data Export (optional). Leave false for a safe first deployment.')
param enableWorkspaceDataExport bool = true

@description('Log Analytics tables to export when enableWorkspaceDataExport=true')
param exportTableNames array = [
  // Typical ADF dedicated tables (adjust to what you actually have in your workspace)
  'ADFActivityRun'
  'ADFPipelineRun'
  'ADFTriggerRun'
]

@description('Enable Stream Analytics job (optional). Leave false if you prefer Databricks Structured Streaming.')
param enableStreamAnalytics bool = true

@description('Event Hub partition count')
@minValue(1)
param eventHubPartitionCount int = 2

@description('Event Hub message retention in days')
@minValue(1)
@maxValue(90)
param eventHubMessageRetentionInDays int = 7

@description('Azure SQL admin login')
param sqlAdminLogin string = 'sqladmin'

@description('Azure SQL admin password')
@secure()
param sqlAdminPassword string

@description('Azure SQL DB name for aggregated stats')
param sqlDbName string = 'logsdb'

@description('Table name used by Stream Analytics output (must exist)')
param sqlOutputTable string = 'RawDiagnostics'

@description('Databricks workspace SKU name')
@allowed([
  'standard'
  'premium'
  'trial'
])
param databricksSkuName string = 'premium'

@description('Databricks public network access')
@allowed([
  'Enabled'
  'Disabled'
])
param databricksPublicNetworkAccess string = 'Enabled'

var uniq = toLower(take(suffix, 6))
var base = toLower(replace(replace('${namePrefix}-${env}', ' ', '-'), '_', '-'))
var stem = '${base}-${uniq}'

// Keep names within common Azure limits (worst-case safe)
var names = {
  law: take('${stem}-law', 63)
  adf: take('${stem}-adf', 63)
  dbw: take('${stem}-dbw', 30)
  ehns: take('${stem}-ehns', 50)
  eh: take('${stem}-hotpath', 50)
  ehSendRule: take('${stem}-eh-send', 50)
  ehListenRule: take('${stem}-eh-listen', 50)
  ehConsumerGroup: take('${stem}-asa', 50)
  adfDiag: take('${stem}-adf-diag', 63)
  dbwDiag: take('${stem}-dbw-diag', 63)
  lawExport: take('${stem}-law-export', 63)
  sqlServer: take('${stem}-sql', 63)
  sqlDb: take(sqlDbName, 128)
  asa: take('${stem}-asa', 63)
  asaInput: 'ehIn'
  asaOutput: 'sqlOut'
  asaTransform: 'transform'
}

var tags = {
  env: env
  workload: namePrefix
}

//
// 1) Log Analytics Workspace
//
resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2022-10-01' = {
  name: names.law
  location: location
  tags: tags
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: logRetentionInDays
    // Workspace data export feature toggle (needed only if you deploy dataExports)
    features: {
      enableDataExport: enableWorkspaceDataExport
    }
  }
}

//
// 2) Event Hubs (hot path)
//
resource eventHubNamespace 'Microsoft.EventHub/namespaces@2025-05-01-preview' = {
  name: names.ehns
  location: location
  tags: tags
  sku: {
    name: 'Standard'
    tier: 'Standard'
    capacity: 1
  }
  properties: {
    isAutoInflateEnabled: true
    maximumThroughputUnits: 2
    zoneRedundant: false
  }
}

resource eventHub 'Microsoft.EventHub/namespaces/eventhubs@2025-05-01-preview' = {
  name: names.eh
  parent: eventHubNamespace
  properties: {
    partitionCount: eventHubPartitionCount
    messageRetentionInDays: eventHubMessageRetentionInDays
  }
}

resource ehSendRule 'Microsoft.EventHub/namespaces/authorizationRules@2025-05-01-preview' = {
  name: names.ehSendRule
  parent: eventHubNamespace
  properties: {
    rights: [
      'Manage'
      'Send'
      'Listen'
    ]
  }
}


resource ehListenRule 'Microsoft.EventHub/namespaces/authorizationRules@2025-05-01-preview' = {
  name: names.ehListenRule
  parent: eventHubNamespace
  properties: {
    rights: [
      'Listen'
    ]
  }
}

resource ehConsumerGroup 'Microsoft.EventHub/namespaces/eventhubs/consumergroups@2025-05-01-preview' = if (enableStreamAnalytics) {
  name: names.ehConsumerGroup
  parent: eventHub
  properties: {}
}

//
// 3) Data Factory + Diagnostics -> Log Analytics + Event Hub
//
resource adf 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: names.adf
  location: location
  tags: tags
  properties: {}
}

// NOTE: Platform metrics export is not supported for all resource types via Diagnostic Settings.
// Metrics blocks were removed to avoid 'Metric export is not enabled' errors (notably on Azure Databricks).
resource adfDiagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: names.adfDiag
  scope: adf
  properties: {
    logAnalyticsDestinationType: 'Dedicated'
    workspaceId: logAnalytics.id
    eventHubAuthorizationRuleId: ehSendRule.id
    eventHubName: eventHub.name
    logs: [
      {
        categoryGroup: 'allLogs'
        enabled: true
        retentionPolicy: {
          enabled: false
          days: 0
        }
      }
    ]
  }
}

//
// 4) Optional: Log Analytics Workspace Data Export -> Event Hub
//    (Use ONLY if you want workspace tables exported. Deploy safely with enableWorkspaceDataExport=false)
//
resource lawDataExport 'Microsoft.OperationalInsights/workspaces/dataExports@2025-07-01' = if (enableWorkspaceDataExport) {
  name: names.lawExport
  parent: logAnalytics
  properties: {
    enable: true
    tableNames: exportTableNames
    destination: {
      resourceId: eventHubNamespace.id
      metaData: {
        eventHubName: eventHub.name
      }
    }
  }
}

//
// 5) Optional: Stream Analytics -> aggregates -> Azure SQL
//
resource sqlServer 'Microsoft.Sql/servers@2022-05-01-preview' = if (enableStreamAnalytics) {
  name: names.sqlServer
  location: location
  tags: tags
  properties: {
    administratorLogin: sqlAdminLogin
    administratorLoginPassword: sqlAdminPassword
  }
}

resource sqlDb 'Microsoft.Sql/servers/databases@2022-05-01-preview' = if (enableStreamAnalytics) {
  name: names.sqlDb
  parent: sqlServer
  location: location
  sku: {
    name: 'Standard'
    tier: 'Standard'
  }
}

resource sqlFirewallAllowAzure 'Microsoft.Sql/servers/firewallRules@2022-05-01-preview' = if (enableStreamAnalytics) {
  name: 'AllowAzureServices'
  parent: sqlServer
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

resource streamJob 'Microsoft.StreamAnalytics/streamingjobs@2021-10-01-preview' = if (enableStreamAnalytics) {
  name: names.asa
  location: location
  tags: tags
  properties: {
    sku: {
      name: 'Standard'
    }
    outputErrorPolicy: 'Stop'
    eventsOutOfOrderPolicy: 'Adjust'
    eventsOutOfOrderMaxDelayInSeconds: 10
    compatibilityLevel: '1.2'
    dataLocale: 'en-US'
  }
}

var ehListenKey = enableStreamAnalytics ? listKeys(ehListenRule.id, '2025-05-01-preview').primaryKey : ''

resource asaInput 'Microsoft.StreamAnalytics/streamingjobs/inputs@2021-10-01-preview' = if (enableStreamAnalytics) {
  name: names.asaInput
  parent: streamJob
  properties: {
    type: 'Stream'
    serialization: {
      type: 'Json'
      properties: {
        encoding: 'UTF8'
        format: 'LineSeparated'
      }
    }
    datasource: {
      type: 'Microsoft.ServiceBus/EventHub'
      properties: {
        authenticationMode: 'ConnectionString'
        serviceBusNamespace: eventHubNamespace.name
        eventHubName: eventHub.name
        consumerGroupName: ehConsumerGroup.name
        sharedAccessPolicyName: ehListenRule.name
        sharedAccessPolicyKey: ehListenKey
      }
    }
  }
}

resource asaOutput 'Microsoft.StreamAnalytics/streamingjobs/outputs@2021-10-01-preview' = if (enableStreamAnalytics) {
  name: names.asaOutput
  parent: streamJob
  properties: {
    serialization: {
      type: 'Json'
      properties: {
        encoding: 'UTF8'
        format: 'LineSeparated'
      }
    }
    datasource: {
      type: 'Microsoft.Sql/Server/Database'
      properties: {
        authenticationMode: 'ConnectionString'
        server: '${sqlServer.name}.database.windows.net'
        database: sqlDb.name
        table: sqlOutputTable
        user: sqlAdminLogin
        password: sqlAdminPassword
      }
    }
  }
}

resource asaTransform 'Microsoft.StreamAnalytics/streamingjobs/transformations@2021-10-01-preview' = if (enableStreamAnalytics) {
  name: names.asaTransform
  parent: streamJob
  properties: {
    streamingUnits: 1
    query: '''
      WITH Flat AS (
        SELECT r.ArrayValue AS rec
        FROM [ehIn] i
        CROSS APPLY GetArrayElements(i.records) AS r
      )
      SELECT
        COALESCE(
          GetRecordPropertyValue(rec, 'resourceProvider'),
          Split(GetRecordPropertyValue(rec, 'operationName'), '/')[0],
          Split(Split(GetRecordPropertyValue(rec, 'resourceId'), '/providers/')[1], '/')[0],
          'unknown'
        ) AS Source,
      
        GetRecordPropertyValue(rec, 'category') AS category,
        TRY_CAST(GetRecordPropertyValue(rec,'time') AS datetime) AS event_time_utc,
        GetRecordPropertyValue(rec, 'resourceId') AS resource_id,
        GetRecordPropertyValue(rec, 'operationName') AS operation_name,
        GetRecordPropertyValue(rec, 'level') AS level,
        GetRecordPropertyValue(rec, 'status') AS status,
        TRY_CAST(GetRecordPropertyValue(rec,'durationMs') AS bigint) AS duration_ms,
        json_stringify(rec) AS payload_json
      INTO [sqlOut]
      FROM Flat;
    '''
  }
}

//
// Outputs (handy when wiring ADF/DBX/queries)
//
output logAnalyticsWorkspaceName string = logAnalytics.name
output logAnalyticsWorkspaceId string = logAnalytics.id
output eventHubNamespaceName string = eventHubNamespace.name
output eventHubName string = eventHub.name
output adfName string = adf.name
output databricksWorkspaceName string = databricksWorkspace.name
