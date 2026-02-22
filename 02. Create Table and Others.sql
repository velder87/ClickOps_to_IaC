      WITH Flat AS (
        SELECT r.ArrayValue AS rec
        FROM [ehIn] i
        CROSS APPLY GetArrayElements(i.records) AS r
      )
      SELECT
        CASE
        -- 1) Si resourceProvider existe
        WHEN GetRecordPropertyValue(rec, 'resourceProvider') IS NOT NULL
            THEN CAST(GetRecordPropertyValue(rec, 'resourceProvider') AS nvarchar(max))

        -- 2) operationName -> premier segment avant '/'
        WHEN GetRecordPropertyValue(rec, 'operationName') IS NOT NULL
            THEN
                CASE
                    WHEN CHARINDEX('/', CAST(GetRecordPropertyValue(rec, 'operationName') AS nvarchar(max))) > 0
                        THEN LEFT(
                            CAST(GetRecordPropertyValue(rec, 'operationName') AS nvarchar(max)),
                            CHARINDEX('/', CAST(GetRecordPropertyValue(rec, 'operationName') AS nvarchar(max))) - 1
                        )
                    ELSE CAST(GetRecordPropertyValue(rec, 'operationName') AS nvarchar(max))
                END

        -- 3) resourceId contient '/providers/' -> extraire providerNamespace
        WHEN GetRecordPropertyValue(rec, 'resourceId') IS NOT NULL
             AND CHARINDEX('/providers/', CAST(GetRecordPropertyValue(rec, 'resourceId') AS nvarchar(max))) > 0
            THEN
                CASE
                    -- position de début juste après '/providers/'
                    WHEN CHARINDEX(
                            '/',
                            CAST(GetRecordPropertyValue(rec, 'resourceId') AS nvarchar(max)),
                            CHARINDEX('/providers/', CAST(GetRecordPropertyValue(rec, 'resourceId') AS nvarchar(max))) + LEN('/providers/')
                         ) > 0
                        THEN SUBSTRING(
                            CAST(GetRecordPropertyValue(rec, 'resourceId') AS nvarchar(max)),
                            CHARINDEX('/providers/', CAST(GetRecordPropertyValue(rec, 'resourceId') AS nvarchar(max))) + LEN('/providers/'),
                            CHARINDEX(
                                '/',
                                CAST(GetRecordPropertyValue(rec, 'resourceId') AS nvarchar(max)),
                                CHARINDEX('/providers/', CAST(GetRecordPropertyValue(rec, 'resourceId') AS nvarchar(max))) + LEN('/providers/')
                            ) - (CHARINDEX('/providers/', CAST(GetRecordPropertyValue(rec, 'resourceId') AS nvarchar(max))) + LEN('/providers/'))
                        )
                    -- s'il n'y a pas de '/' après, on prend le reste
                    ELSE SUBSTRING(
                        CAST(GetRecordPropertyValue(rec, 'resourceId') AS nvarchar(max)),
                        CHARINDEX('/providers/', CAST(GetRecordPropertyValue(rec, 'resourceId') AS nvarchar(max))) + LEN('/providers/'),
                        LEN(CAST(GetRecordPropertyValue(rec, 'resourceId') AS nvarchar(max)))
                    )
                END

        ELSE 'unknown'
    END AS Source,
      
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

CREATE TABLE dbo.RawDiagnostics (
  id              bigint IDENTITY(1,1) PRIMARY KEY,
  ingest_utc       datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),

  source          nvarchar(32)  NOT NULL,  -- 'adf' / 'databricks' / 'custom'
  category        nvarchar(128) NULL,
  event_time_utc  datetime2(3)  NULL,
  resource_id     nvarchar(512) NULL,
  operation_name  nvarchar(256) NULL,
  level           nvarchar(32)  NULL,
  status          nvarchar(64)  NULL,
  duration_ms     bigint        NULL,

  payload_json    nvarchar(max) NOT NULL
);

CREATE INDEX IX_RawDiagnostics_eventtime ON dbo.RawDiagnostics(event_time_utc);
CREATE INDEX IX_RawDiagnostics_category  ON dbo.RawDiagnostics(category);
