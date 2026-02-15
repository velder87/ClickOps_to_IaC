SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   VIEW [dbo].[vw_RawDiagnostics_Unwrapped]
AS
SELECT
  TRY_CONVERT(bigint, r.id) AS RawId,
  r.ingest_utc              AS IngestUtc,
  r.source                  AS Source,
  r.category                AS Category,
  r.resource_id             AS ResourceId,
  r.operation_name          AS OperationName,
  r.level                   AS Level,
  r.status                  AS Status,
  r.duration_ms             AS DurationMs,

  -- JSON complet original
  r.payload_json            AS PayloadJson,

  -- JSON "objet" (si payload_json = " {...} " alors on unwrap)
  CASE
    WHEN LEFT(LTRIM(r.payload_json),1) = '"'
      THEN JSON_VALUE(r.payload_json, '$')
    ELSE r.payload_json
  END AS PayloadObjJson
FROM dbo.RawDiagnostics r;
GO

CREATE   VIEW [dbo].[vw_ADF_ActivityRuns_Normalized]
AS
WITH B AS (
    SELECT
        u.RawId,
        u.IngestUtc,
        COALESCE(u.Source, 'adf') AS Source,
        u.Category,
        u.ResourceId,
        u.OperationName,
        u.Level,
        u.Status,
        u.PayloadObjJson,

        -- Calculs temps (dans le CTE => réutilisable ensuite)
        COALESCE(
            TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.time'), 'Z','')),
            TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.env_time'), 'Z','')),
            TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.TimeGenerated'), 'Z',''))
        ) AS EventTimeUtc,

        COALESCE(
            TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.start'), 'Z','')),
            TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.Start'), 'Z',''))
        ) AS StartUtc,

        COALESCE(
            TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.end'), 'Z','')),
            TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.End'), 'Z',''))
        ) AS EndUtc
    FROM dbo.vw_RawDiagnostics_Unwrapped u
    WHERE u.Category = 'ActivityRuns'
),
P AS (
    SELECT
        b.*,

        COALESCE(JSON_VALUE(b.PayloadObjJson,'$.activityName'),
                 JSON_VALUE(b.PayloadObjJson,'$.ActivityName')) AS ActivityName,

        COALESCE(JSON_VALUE(b.PayloadObjJson,'$.activityType'),
                 JSON_VALUE(b.PayloadObjJson,'$.ActivityType')) AS ActivityType,

        COALESCE(JSON_VALUE(b.PayloadObjJson,'$.activityRunId'),
                 JSON_VALUE(b.PayloadObjJson,'$.ActivityRunId')) AS ActivityRunId,

        COALESCE(JSON_VALUE(b.PayloadObjJson,'$.pipelineName'),
                 JSON_VALUE(b.PayloadObjJson,'$.PipelineName')) AS PipelineName,

        COALESCE(JSON_VALUE(b.PayloadObjJson,'$.pipelineRunId'),
                 JSON_VALUE(b.PayloadObjJson,'$.PipelineRunId'),
                 JSON_VALUE(b.PayloadObjJson,'$.correlationId'),
                 JSON_VALUE(b.PayloadObjJson,'$.CorrelationId')) AS PipelineRunId,

        COALESCE(
            JSON_QUERY(b.PayloadObjJson,'$.properties.Input'),
            CASE WHEN ISJSON(JSON_VALUE(b.PayloadObjJson,'$.Input')) = 1 THEN JSON_VALUE(b.PayloadObjJson,'$.Input') END
        ) AS InputJson,

        COALESCE(
            JSON_QUERY(b.PayloadObjJson,'$.properties.Output'),
            CASE WHEN ISJSON(JSON_VALUE(b.PayloadObjJson,'$.Output')) = 1 THEN JSON_VALUE(b.PayloadObjJson,'$.Output') END
        ) AS OutputJson,

        COALESCE(
            JSON_QUERY(b.PayloadObjJson,'$.properties.Error'),
            CASE WHEN ISJSON(JSON_VALUE(b.PayloadObjJson,'$.Error')) = 1 THEN JSON_VALUE(b.PayloadObjJson,'$.Error') END
        ) AS ErrorJson,

        CASE
            WHEN ISJSON(JSON_QUERY(b.PayloadObjJson,'$.tags')) = 1 THEN JSON_QUERY(b.PayloadObjJson,'$.tags')
            WHEN ISJSON(JSON_VALUE(b.PayloadObjJson,'$.tags')) = 1 THEN JSON_VALUE(b.PayloadObjJson,'$.tags')
            WHEN ISJSON(JSON_QUERY(b.PayloadObjJson,'$.Tags')) = 1 THEN JSON_QUERY(b.PayloadObjJson,'$.Tags')
            WHEN ISJSON(JSON_VALUE(b.PayloadObjJson,'$.Tags')) = 1 THEN JSON_VALUE(b.PayloadObjJson,'$.Tags')
            ELSE NULL
        END AS TagsJson
    FROM B b
)
SELECT
    RawId,
    IngestUtc,
    EventTimeUtc,
    StartUtc,
    EndUtc,
    Source,

    ActivityName,
    ActivityType,
    ActivityRunId,
    PipelineName,
    PipelineRunId,

    COALESCE(JSON_VALUE(PayloadObjJson,'$.status'),
             JSON_VALUE(PayloadObjJson,'$.Status'),
             Status) AS Status,

    Level,
    Category,
    ResourceId,
    OperationName,

    CASE
        WHEN EventTimeUtc IS NOT NULL THEN DATEDIFF(SECOND, EventTimeUtc, IngestUtc)
        ELSE NULL
    END AS IngestionLatencySec,

    CASE
        WHEN StartUtc IS NOT NULL AND EndUtc IS NOT NULL AND EndUtc > '1900-01-01'
        THEN DATEDIFF(SECOND, StartUtc, EndUtc)
        ELSE NULL
    END AS DurationSecDerived,

    TRY_CONVERT(bigint, JSON_VALUE(OutputJson,'$.rowsCopied')) AS RowsCopied,
    TRY_CONVERT(int,    JSON_VALUE(OutputJson,'$.copyDuration')) AS CopyDurationSec,
    TRY_CONVERT(int,    JSON_VALUE(OutputJson,'$.executionDetails[0].profile.queue.duration')) AS QueuingDurationSec,

    COALESCE(
        JSON_VALUE(ErrorJson,'$.message'),
        JSON_VALUE(ErrorJson,'$.Message'),
        JSON_VALUE(ErrorJson,'$.details')
    ) AS ErrorMessage,

    InputJson,
    OutputJson,
    ErrorJson,
    TagsJson,

    PayloadObjJson AS PayloadJson
FROM P;
GO

CREATE   VIEW [dbo].[vw_ADF_PipelineRuns_Normalized]
AS
SELECT
  u.RawId,
  u.IngestUtc,

  COALESCE(
    TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.time'), 'Z','')),
    TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.TimeGenerated'), 'Z','')),
    TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.env_time'), 'Z',''))
  ) AS EventTimeUtc,

  COALESCE(
    TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.start'), 'Z','')),
    TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.Start'), 'Z',''))
  ) AS StartUtc,

  COALESCE(
    TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.end'), 'Z','')),
    TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.End'), 'Z',''))
  ) AS EndUtc,

  COALESCE(u.Source, 'adf') AS Source,

  -- Identifiants
  COALESCE(
    JSON_VALUE(u.PayloadObjJson,'$.runId'),
    JSON_VALUE(u.PayloadObjJson,'$.RunId'),
    JSON_VALUE(u.PayloadObjJson,'$.pipelineRunId'),
    JSON_VALUE(u.PayloadObjJson,'$.PipelineRunId'),
    JSON_VALUE(u.PayloadObjJson,'$.correlationId'),
    JSON_VALUE(u.PayloadObjJson,'$.CorrelationId')
  ) AS RunId,

  COALESCE(
    JSON_VALUE(u.PayloadObjJson,'$.pipelineName'),
    JSON_VALUE(u.PayloadObjJson,'$.PipelineName')
  ) AS PipelineName,

  -- Status / Level / Category / Resource
  COALESCE(
    JSON_VALUE(u.PayloadObjJson,'$.status'),
    JSON_VALUE(u.PayloadObjJson,'$.Status'),
    u.Status
  ) AS Status,

  u.Level,
  u.Category,
  u.ResourceId,
  u.OperationName,

  -- Metrics
  CASE
    WHEN COALESCE(
      TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.time'), 'Z','')),
      TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.TimeGenerated'), 'Z','')),
      TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.env_time'), 'Z',''))
    ) IS NOT NULL
    THEN DATEDIFF(
      SECOND,
      COALESCE(
        TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.time'), 'Z','')),
        TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.TimeGenerated'), 'Z','')),
        TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.env_time'), 'Z',''))
      ),
      u.IngestUtc
    )
    ELSE NULL
  END AS IngestionLatencySec,

  CASE
    WHEN
      COALESCE(
        TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.start'), 'Z','')),
        TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.Start'), 'Z',''))
      ) IS NOT NULL
      AND
      COALESCE(
        TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.end'), 'Z','')),
        TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.End'), 'Z',''))
      ) > '1900-01-01'
    THEN DATEDIFF(
      SECOND,
      COALESCE(
        TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.start'), 'Z','')),
        TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.Start'), 'Z',''))
      ),
      COALESCE(
        TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.end'), 'Z','')),
        TRY_CONVERT(datetime2(7), REPLACE(JSON_VALUE(u.PayloadObjJson,'$.End'), 'Z',''))
      )
    )
    ELSE NULL
  END AS DurationSecDerived,

  -- TagsJson (peut être objet ou string JSON)
  CASE
    WHEN ISJSON(JSON_QUERY(u.PayloadObjJson,'$.tags')) = 1 THEN JSON_QUERY(u.PayloadObjJson,'$.tags')
    WHEN ISJSON(JSON_VALUE(u.PayloadObjJson,'$.tags')) = 1 THEN JSON_VALUE(u.PayloadObjJson,'$.tags')
    WHEN ISJSON(JSON_QUERY(u.PayloadObjJson,'$.Tags')) = 1 THEN JSON_QUERY(u.PayloadObjJson,'$.Tags')
    WHEN ISJSON(JSON_VALUE(u.PayloadObjJson,'$.Tags')) = 1 THEN JSON_VALUE(u.PayloadObjJson,'$.Tags')
    ELSE NULL
  END AS TagsJson,

  -- JSON complet pour drilldown
  u.PayloadObjJson AS PayloadJson

FROM dbo.vw_RawDiagnostics_Unwrapped u
WHERE u.Category = 'PipelineRuns';
GO

CREATE   VIEW [dbo].[vw_EventsNormalized]
AS
-- pipeline_run
SELECT
  RawId,
  IngestUtc,
  EventTimeUtc,
  StartUtc,
  EndUtc,
  Source,
  'pipeline_run' AS EntityType,
  PipelineName   AS EntityName,
  RunId          AS RunId,
  RunId          AS PipelineRunId,
  CAST(NULL AS nvarchar(128)) AS ActivityRunId,
  CAST(NULL AS nvarchar(64))  AS ActivityType,
  Status,
  Level,
  Category,
  ResourceId,
  OperationName,
  IngestionLatencySec,
  DurationSecDerived,
  CAST(NULL AS bigint) AS RowsCopied,
  CAST(NULL AS int)    AS CopyDurationSec,
  CAST(NULL AS int)    AS QueuingDurationSec,
  CAST(NULL AS nvarchar(max)) AS ErrorMessage,
  CAST(NULL AS nvarchar(max)) AS InputJson,
  CAST(NULL AS nvarchar(max)) AS OutputJson,
  CAST(NULL AS nvarchar(max)) AS ErrorJson,
  TagsJson,
  PayloadJson
FROM dbo.vw_ADF_PipelineRuns_Normalized

UNION ALL

-- activity_run
SELECT
  RawId,
  IngestUtc,
  EventTimeUtc,
  StartUtc,
  EndUtc,
  Source,
  'activity_run' AS EntityType,
  ActivityName   AS EntityName,
  ActivityRunId  AS RunId,
  PipelineRunId  AS PipelineRunId,
  ActivityRunId  AS ActivityRunId,
  ActivityType,
  Status,
  Level,
  Category,
  ResourceId,
  OperationName,
  IngestionLatencySec,
  DurationSecDerived,
  RowsCopied,
  CopyDurationSec,
  QueuingDurationSec,
  ErrorMessage,
  InputJson,
  OutputJson,
  ErrorJson,
  TagsJson,
  PayloadJson
FROM dbo.vw_ADF_ActivityRuns_Normalized;
GO

CREATE   VIEW [dbo].[vw_ActivityRuns_Latest]
AS
WITH X AS (
  SELECT
    Source,
    EntityName        AS PipelineName,
    PipelineRunId,
    Status,
    EventTimeUtc,
    IngestUtc,
    DurationSecDerived,
    IngestionLatencySec,
    ResourceId,
    OperationName,
    PayloadJson,
    ROW_NUMBER() OVER (
      PARTITION BY PipelineRunId
      ORDER BY EventTimeUtc DESC, IngestUtc DESC
    ) AS rn
  FROM dbo.vw_EventsNormalized
  WHERE EntityType = 'activity_run'
    AND PipelineRunId IS NOT NULL
)
SELECT *
FROM X
WHERE rn = 1;
GO

CREATE   VIEW [dbo].[vw_PipelineRuns_Latest]
AS
WITH X AS (
  SELECT
    Source,
    EntityName        AS PipelineName,
    PipelineRunId,
    Status,
    EventTimeUtc,
    IngestUtc,
    DurationSecDerived,
    IngestionLatencySec,
    ResourceId,
    OperationName,
    PayloadJson,
    ROW_NUMBER() OVER (
      PARTITION BY PipelineRunId
      ORDER BY EventTimeUtc DESC, IngestUtc DESC
    ) AS rn
  FROM dbo.vw_EventsNormalized
  WHERE EntityType = 'pipeline_run'
    AND PipelineRunId IS NOT NULL
)
SELECT *
FROM X
WHERE rn = 1;
GO

CREATE   VIEW [dbo].[vw_FactEvents_Keyed]
AS
WITH E AS (
    SELECT
        e.RawId,
        e.IngestUtc,
        e.EventTimeUtc,
        e.StartUtc,
        e.EndUtc,

        e.Source,
        e.EntityType,
        e.EntityName,
        e.RunId,

        e.PipelineRunId,
        e.ActivityRunId,
        e.ActivityType,

        e.Status,
        e.Level,
        e.Category,
        e.ResourceId,
        e.OperationName,

        e.IngestionLatencySec,
        e.DurationSecDerived,

        e.RowsCopied,
        e.CopyDurationSec,
        e.QueuingDurationSec,

        e.ErrorMessage,

        e.InputJson,
        e.OutputJson,
        e.ErrorJson,

        e.TagsJson,
        e.PayloadJson,

        -- DateKey (UTC) : yyyymmdd
        CASE
            WHEN e.EventTimeUtc IS NOT NULL
            THEN CONVERT(int, FORMAT(CAST(e.EventTimeUtc AS date), 'yyyyMMdd'))
            ELSE NULL
        END AS DateKey
    FROM dbo.vw_EventsNormalized e
)
SELECT
    E.RawId,

    -- clés dimensions
    E.DateKey,
    ds.SourceKey,
    de.EntityKey,
    dstat.StatusKey,
    dsev.SeverityKey,
    dcat.CategoryKey,
    dr.ResourceKey,
    dop.OperationKey,

    -- faits temporels
    E.EventTimeUtc,
    E.IngestUtc,
    E.StartUtc,
    E.EndUtc,

    -- faits numériques
    E.IngestionLatencySec,
    E.DurationSecDerived,
    E.RowsCopied,
    E.CopyDurationSec,
    E.QueuingDurationSec,

    -- attributs de drill-down (tu peux en masquer certains dans PBI)
    E.RunId,
    E.PipelineRunId,
    E.ActivityRunId,
    E.ActivityType,

    -- texte / diagnostic
    E.ErrorMessage,
    E.InputJson,
    E.OutputJson,
    E.ErrorJson,
    E.TagsJson,
    E.PayloadJson

FROM E
LEFT JOIN dbo.DimSource   ds   ON ds.Source = E.Source
LEFT JOIN dbo.DimEntity   de   ON de.EntityType = E.EntityType AND de.EntityName = E.EntityName
LEFT JOIN dbo.DimStatus   dstat ON dstat.Status = E.Status
LEFT JOIN dbo.DimSeverity dsev  ON dsev.Severity = E.Level
LEFT JOIN dbo.DimCategory dcat  ON dcat.Category = E.Category
LEFT JOIN dbo.DimResource dr    ON dr.ResourceId = E.ResourceId
LEFT JOIN dbo.DimOperation dop  ON dop.OperationName = E.OperationName;
GO

CREATE   PROCEDURE [dbo].[usp_Load_DimCategory]
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.DimCategory AS tgt
    USING (
        SELECT DISTINCT Category
        FROM dbo.vw_EventsNormalized
        WHERE Category IS NOT NULL
    ) AS src
    ON tgt.Category = src.Category
    WHEN NOT MATCHED THEN
      INSERT (Category) VALUES (src.Category);
END;
GO

CREATE   PROCEDURE [dbo].[usp_Load_DimDate]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start date = (
        SELECT MIN(CAST(EventTimeUtc AS date))
        FROM dbo.vw_EventsNormalized
        WHERE EventTimeUtc IS NOT NULL
    );

    DECLARE @end date = (
        SELECT MAX(CAST(EventTimeUtc AS date))
        FROM dbo.vw_EventsNormalized
        WHERE EventTimeUtc IS NOT NULL
    );

    IF @start IS NULL OR @end IS NULL
    BEGIN
        -- fallback POC : 30 jours
        SET @end = CAST(SYSUTCDATETIME() AS date);
        SET @start = DATEADD(DAY, -30, @end);
    END

    -- Rebuild simple (POC)
    TRUNCATE TABLE dbo.DimDate;

    ;WITH N AS (
        SELECT TOP (DATEDIFF(DAY, @start, @end) + 1)
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
        FROM sys.all_objects
    ),
    D AS (
        SELECT DATEADD(DAY, n, @start) AS d
        FROM N
    )
    INSERT dbo.DimDate (DateKey, [Date], [Year], [Quarter], [MonthNumber], [MonthName],
                        [WeekOfYear], [DayOfMonth], [DayOfWeek], [DayName])
    SELECT
        CONVERT(int, FORMAT(d, 'yyyyMMdd')) AS DateKey,
        d,
        DATEPART(YEAR, d),
        DATEPART(QUARTER, d),
        DATEPART(MONTH, d),
        DATENAME(MONTH, d),
        DATEPART(ISO_WEEK, d),
        DATEPART(DAY, d),
        DATEPART(WEEKDAY, d),
        DATENAME(WEEKDAY, d)
    FROM D;
END;
GO

CREATE   PROCEDURE [dbo].[usp_Load_DimEntity]
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.DimEntity AS tgt
    USING (
        SELECT DISTINCT EntityType, EntityName
        FROM dbo.vw_EventsNormalized
        WHERE EntityType IS NOT NULL AND EntityName IS NOT NULL
    ) AS src
    ON tgt.EntityType = src.EntityType AND tgt.EntityName = src.EntityName
    WHEN NOT MATCHED THEN
      INSERT (EntityType, EntityName) VALUES (src.EntityType, src.EntityName);
END;
GO

CREATE   PROCEDURE [dbo].[usp_Load_DimOperation]
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.DimOperation AS tgt
    USING (
        SELECT DISTINCT OperationName
        FROM dbo.vw_EventsNormalized
        WHERE OperationName IS NOT NULL
    ) src
    ON tgt.OperationName = src.OperationName
    WHEN NOT MATCHED THEN
      INSERT (OperationName) VALUES (src.OperationName);
END;
GO

CREATE   PROCEDURE [dbo].[usp_Load_DimResource]
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH R AS (
        SELECT DISTINCT ResourceId
        FROM dbo.vw_EventsNormalized
        WHERE ResourceId IS NOT NULL
    ),
    P AS (
        SELECT
            ResourceId,

            -- /subscriptions/{sub}/resourceGroups/{rg}/providers/{prov}/{type}/{name}
            CASE
                WHEN CHARINDEX('/subscriptions/', ResourceId) > 0
                     AND CHARINDEX('/', ResourceId, CHARINDEX('/subscriptions/', ResourceId) + LEN('/subscriptions/')) > 0
                THEN SUBSTRING(
                    ResourceId,
                    CHARINDEX('/subscriptions/', ResourceId) + LEN('/subscriptions/'),
                    CHARINDEX('/', ResourceId, CHARINDEX('/subscriptions/', ResourceId) + LEN('/subscriptions/'))
                      - (CHARINDEX('/subscriptions/', ResourceId) + LEN('/subscriptions/'))
                )
                ELSE NULL
            END AS SubscriptionId,

            CASE
                WHEN CHARINDEX('/resourceGroups/', ResourceId) > 0
                     AND CHARINDEX('/', ResourceId, CHARINDEX('/resourceGroups/', ResourceId) + LEN('/resourceGroups/')) > 0
                THEN SUBSTRING(
                    ResourceId,
                    CHARINDEX('/resourceGroups/', ResourceId) + LEN('/resourceGroups/'),
                    CHARINDEX('/', ResourceId, CHARINDEX('/resourceGroups/', ResourceId) + LEN('/resourceGroups/'))
                      - (CHARINDEX('/resourceGroups/', ResourceId) + LEN('/resourceGroups/'))
                )
                ELSE NULL
            END AS ResourceGroupName,

            CASE
                WHEN CHARINDEX('/providers/', ResourceId) > 0
                     AND CHARINDEX('/', ResourceId, CHARINDEX('/providers/', ResourceId) + LEN('/providers/')) > 0
                THEN SUBSTRING(
                    ResourceId,
                    CHARINDEX('/providers/', ResourceId) + LEN('/providers/'),
                    CHARINDEX('/', ResourceId, CHARINDEX('/providers/', ResourceId) + LEN('/providers/'))
                      - (CHARINDEX('/providers/', ResourceId) + LEN('/providers/'))
                )
                ELSE NULL
            END AS ProviderNamespace,

            CASE
                WHEN ResourceId LIKE '%/%'
                THEN RIGHT(ResourceId, CHARINDEX('/', REVERSE(ResourceId)) - 1)
                ELSE NULL
            END AS ResourceName
        FROM R
    ),
    P2 AS (
        SELECT
            ResourceId,
            SubscriptionId,
            ResourceGroupName,
            ProviderNamespace,
            ResourceName,

            CASE
                WHEN ProviderNamespace IS NULL OR ResourceName IS NULL THEN NULL
                WHEN CHARINDEX(ProviderNamespace + '/', ResourceId) = 0 THEN NULL
                ELSE
                    -- extrait ce qu'il y a après ProviderNamespace + '/'
                    -- puis retire le dernier segment "/{ResourceName}"
                    CASE
                        WHEN LEN(SUBSTRING(
                                ResourceId,
                                CHARINDEX(ProviderNamespace + '/', ResourceId) + LEN(ProviderNamespace + '/'),
                                LEN(ResourceId)
                            )) > (LEN(ResourceName) + 1)
                        THEN LEFT(
                            SUBSTRING(
                                ResourceId,
                                CHARINDEX(ProviderNamespace + '/', ResourceId) + LEN(ProviderNamespace + '/'),
                                LEN(ResourceId)
                            ),
                            LEN(
                                SUBSTRING(
                                    ResourceId,
                                    CHARINDEX(ProviderNamespace + '/', ResourceId) + LEN(ProviderNamespace + '/'),
                                    LEN(ResourceId)
                                )
                            ) - (LEN(ResourceName) + 1)
                        )
                        ELSE NULL
                    END
            END AS ResourceType
        FROM P
    )
    MERGE dbo.DimResource AS tgt
    USING P2 AS src
      ON tgt.ResourceId = src.ResourceId
    WHEN NOT MATCHED THEN
      INSERT (ResourceId, SubscriptionId, ResourceGroupName, ProviderNamespace, ResourceType, ResourceName)
      VALUES (src.ResourceId, src.SubscriptionId, src.ResourceGroupName, src.ProviderNamespace, src.ResourceType, src.ResourceName);
END;
GO

CREATE   PROCEDURE [dbo].[usp_Load_DimSeverity]
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.DimSeverity AS tgt
    USING (
        SELECT DISTINCT [Level] AS Severity
        FROM dbo.vw_EventsNormalized
        WHERE [Level] IS NOT NULL
    ) AS src
    ON tgt.Severity = src.Severity
    WHEN NOT MATCHED THEN
      INSERT (Severity) VALUES (src.Severity);
END;
GO

CREATE   PROCEDURE [dbo].[usp_Load_DimSource]
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.DimSource AS tgt
    USING (
        SELECT DISTINCT Source
        FROM dbo.vw_EventsNormalized
        WHERE Source IS NOT NULL
    ) AS src
    ON tgt.Source = src.Source
    WHEN NOT MATCHED THEN
      INSERT (Source) VALUES (src.Source);
END;
GO

CREATE   PROCEDURE [dbo].[usp_Load_DimStatus]
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.DimStatus AS tgt
    USING (
        SELECT DISTINCT Status
        FROM dbo.vw_EventsNormalized
        WHERE Status IS NOT NULL
    ) AS src
    ON tgt.Status = src.Status
    WHEN NOT MATCHED THEN
      INSERT (Status) VALUES (src.Status);
END;
GO

CREATE   PROCEDURE [dbo].[usp_Load_AllDimensions]
AS
BEGIN
    SET NOCOUNT ON;

    EXEC dbo.usp_Load_DimDate;
    EXEC dbo.usp_Load_DimSource;
    EXEC dbo.usp_Load_DimStatus;
    EXEC dbo.usp_Load_DimSeverity;
    EXEC dbo.usp_Load_DimCategory;
    EXEC dbo.usp_Load_DimEntity;
    EXEC dbo.usp_Load_DimResource;
    EXEC dbo.usp_Load_DimOperation;
END;
GO




