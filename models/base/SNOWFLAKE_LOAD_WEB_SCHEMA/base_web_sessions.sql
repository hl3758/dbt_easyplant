SELECT
    SESSION_ID,
    CAST(CLIENT_ID AS STRING) AS CLIENT_ID,
    OS,
    IP,
    SESSION_AT AS SESSION_AT_TS,
    _FIVETRAN_ID,
    _FIVETRAN_DELETED AS _FIVETRAN_DELETED_BOOLEAN,
    _FIVETRAN_SYNCED AS _FIVETRAN_SYNCED_TS
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY SESSION_ID ORDER BY SESSION_AT) AS ROW_NUM
    FROM {{ source('SNOWFLAKE_LOAD_WEB_SCHEMA', 'SESSIONS') }}
)
WHERE ROW_NUM = 1
