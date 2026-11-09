// Live Football Scores: Azure topology.
//
//   clients ──► Event Hubs (match-events, partition key = matchId)
//                 ├─ consumer group "scoreboard" ──► Consumer (Container App) ──► Table Storage (matchevents, matchstatus, ingestionstats)
//                 │                                                          └─► Blob (rejected-events, checkpoints)
//                 └─ consumer group "highlights" ──► highlights / stats reader (replays full history)
//               Event Hubs Capture ──► Blob (capture) = long-term match history beyond retention
//   Scoreboard API (Container App) ──► Table Storage (read only)
//
// Validate:  az bicep build --file infra/main.bicep && az bicep lint --file infra/main.bicep

targetScope = 'resourceGroup'

@description('Short prefix used in resource names.')
@minLength(3)
@maxLength(10)
param prefix string = 'livescores'

@description('Deployment environment.')
@allowed([
  'dev'
  'prod'
])
param environmentName string = 'dev'

param location string = resourceGroup().location

@description('Partitions bound per-match ordering and max parallel consumers. Cannot be changed later on Standard tier.')
@minValue(2)
@maxValue(32)
param partitionCount int = 8

@description('Throughput units at rest; auto-inflate raises this for the Saturday 3pm burst.')
@minValue(1)
@maxValue(20)
param throughputUnits int = 1

@description('Auto-inflate ceiling for spiky kickoffs.')
@minValue(1)
@maxValue(20)
param maxThroughputUnits int = 10

@description('Container image for the consumer.')
param consumerImage string = 'mcr.microsoft.com/dotnet/samples:aspnetapp'

@description('Container image for the scoreboard API.')
param apiImage string = 'mcr.microsoft.com/dotnet/samples:aspnetapp'

var suffix = uniqueString(resourceGroup().id, prefix, environmentName)
var eventHubName = 'match-events'
var tags = {
  app: 'live-scores'
  env: environmentName
}

// ---------- Observability ----------

resource logs 'Microsoft.OperationalInsights/workspaces@2025-07-01' = {
  name: '${prefix}-${environmentName}-logs'
  location: location
  tags: tags
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: 30
  }
}

// ---------- Storage: history + status (Table), checkpoints / rejects / capture (Blob) ----------

resource storage 'Microsoft.Storage/storageAccounts@2025-06-01' = {
  name: take('${prefix}${environmentName}${suffix}', 24)
  location: location
  tags: tags
  kind: 'StorageV2'
  sku: {
    name: environmentName == 'prod' ? 'Standard_ZRS' : 'Standard_LRS'
  }
  properties: {
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false // apps use managed identity
    supportsHttpsTrafficOnly: true
  }
}

resource tableService 'Microsoft.Storage/storageAccounts/tableServices@2025-06-01' = {
  parent: storage
  name: 'default'
}

// History: one row per event, PK = matchId, RK = eventId (the key is the dedupe).
resource matchEventsTable 'Microsoft.Storage/storageAccounts/tableServices/tables@2025-06-01' = {
  parent: tableService
  name: 'matchevents'
}

// Status: one row per match, what the scoreboard reads.
resource matchStatusTable 'Microsoft.Storage/storageAccounts/tableServices/tables@2025-06-01' = {
  parent: tableService
  name: 'matchstatus'
}

resource statsTable 'Microsoft.Storage/storageAccounts/tableServices/tables@2025-06-01' = {
  parent: tableService
  name: 'ingestionstats'
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2025-06-01' = {
  parent: storage
  name: 'default'
}

resource containers 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-06-01' = [
  for name in [
    'checkpoints'
    'rejected-events'
    'capture'
  ]: {
    parent: blobService
    name: name
  }
]

// ---------- Ingestion: Event Hubs ----------

resource eventHubsNamespace 'Microsoft.EventHub/namespaces@2026-01-01' = {
  name: '${prefix}-${environmentName}-${suffix}'
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned' // used by Capture to write to storage (shared keys are disabled)
  }
  sku: {
    name: 'Standard'
    tier: 'Standard'
    capacity: throughputUnits
  }
  properties: {
    isAutoInflateEnabled: true
    maximumThroughputUnits: maxThroughputUnits
    disableLocalAuth: true // no SAS keys in Azure; managed identity only
    minimumTlsVersion: '1.2'
  }
}

resource eventHub 'Microsoft.EventHub/namespaces/eventhubs@2026-01-01' = {
  parent: eventHubsNamespace
  name: eventHubName
  properties: {
    partitionCount: partitionCount
    messageRetentionInDays: 7 // highlights can replay a whole weekend straight from the hub
    captureDescription: {
      enabled: true
      encoding: 'Avro'
      intervalInSeconds: 300
      sizeLimitInBytes: 314572800
      skipEmptyArchives: true
      destination: {
        name: 'EventHubArchive.AzureBlockBlob'
        identity: {
          type: 'SystemAssigned'
        }
        properties: {
          storageAccountResourceId: storage.id
          blobContainer: 'capture'
          archiveNameFormat: '{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}'
        }
      }
    }
  }
  dependsOn: [
    containers
    captureWritesBlobs
  ]
}

resource scoreboardGroup 'Microsoft.EventHub/namespaces/eventhubs/consumergroups@2026-01-01' = {
  parent: eventHub
  name: 'scoreboard'
}

resource highlightsGroup 'Microsoft.EventHub/namespaces/eventhubs/consumergroups@2026-01-01' = {
  parent: eventHub
  name: 'highlights'
}

// ---------- Compute: Container Apps ----------

resource appsEnv 'Microsoft.App/managedEnvironments@2025-07-01' = {
  name: '${prefix}-${environmentName}-env'
  location: location
  tags: tags
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logs.properties.customerId
        sharedKey: logs.listKeys().primarySharedKey
      }
    }
  }
}

resource consumer 'Microsoft.App/containerApps@2025-07-01' = {
  name: '${prefix}-${environmentName}-consumer'
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    managedEnvironmentId: appsEnv.id
    configuration: {
      activeRevisionsMode: 'Single'
    }
    template: {
      containers: [
        {
          name: 'consumer'
          image: consumerImage
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
          env: [
            { name: 'EventHubs__FullyQualifiedNamespace', value: '${eventHubsNamespace.name}.servicebus.windows.net' }
            { name: 'EventHubs__EventHubName', value: eventHubName }
            { name: 'EventHubs__ConsumerGroup', value: scoreboardGroup.name }
            { name: 'Storage__AccountName', value: storage.name }
          ]
        }
      ]
      // More replicas than partitions is wasted: partitions are the unit of parallelism.
      scale: {
        minReplicas: 1
        maxReplicas: partitionCount
      }
    }
  }
}

resource api 'Microsoft.App/containerApps@2025-07-01' = {
  name: '${prefix}-${environmentName}-api'
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    managedEnvironmentId: appsEnv.id
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: {
        external: true
        targetPort: 8080
        transport: 'auto'
      }
    }
    template: {
      containers: [
        {
          name: 'api'
          image: apiImage
          resources: {
            cpu: json('0.25')
            memory: '0.5Gi'
          }
          env: [
            { name: 'Storage__AccountName', value: storage.name }
          ]
          probes: [
            {
              type: 'Liveness'
              httpGet: {
                path: '/healthz'
                port: 8080
              }
            }
          ]
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 10
        rules: [
          {
            name: 'http'
            http: {
              metadata: {
                concurrentRequests: '100'
              }
            }
          }
        ]
      }
    }
  }
}

// ---------- RBAC (least privilege per app) ----------

var roles = {
  eventHubsDataReceiver: 'a638d3c7-ab3a-418d-83e6-5f17a39d4fde'
  storageBlobDataContributor: 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
  storageTableDataContributor: '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3'
  storageTableDataReader: '76199698-9eea-4c19-bc75-cec21354c6b6'
}

resource consumerReceivesEvents 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(eventHub.id, consumer.id, roles.eventHubsDataReceiver)
  scope: eventHub
  properties: {
    principalId: consumer.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', roles.eventHubsDataReceiver)
  }
}

resource consumerWritesBlobs 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, consumer.id, roles.storageBlobDataContributor)
  scope: storage
  properties: {
    principalId: consumer.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', roles.storageBlobDataContributor)
  }
}

resource consumerWritesTables 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, consumer.id, roles.storageTableDataContributor)
  scope: storage
  properties: {
    principalId: consumer.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', roles.storageTableDataContributor)
  }
}

resource captureWritesBlobs 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, eventHubsNamespace.id, roles.storageBlobDataContributor)
  scope: storage
  properties: {
    principalId: eventHubsNamespace.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', roles.storageBlobDataContributor)
  }
}

resource apiReadsTables'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, api.id, roles.storageTableDataReader)
  scope: storage
  properties: {
    principalId: api.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', roles.storageTableDataReader)
  }
}

// ---------- Outputs ----------

output eventHubsNamespace string = '${eventHubsNamespace.name}.servicebus.windows.net'
output eventHubName string = eventHub.name
output storageAccountName string = storage.name
output apiUrl string = 'https://${api.properties.configuration.ingress.fqdn}'
