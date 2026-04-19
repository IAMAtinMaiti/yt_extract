{{
    config(
        materialized        = 'incremental',
        incremental_strategy = 'append',
        on_schema_change    = 'append_new_columns',
    )
}}

/*
    Staging model — append-only incremental.

    On the very first run every JSON snapshot file in the datalake is loaded.
    On every subsequent run ONLY snapshot files whose path is not already
    present in the table are processed, so rows are never duplicated and the
    table grows monotonically with each new Airflow execution.

    The datalake_path variable is injected at runtime (e.g. from Airflow):
        dbt run --vars '{"datalake_path": "/abs/path/to/datalake"}'
*/

with raw as (

    select
        title,
        channel,
        views,
        upload_time,
        video_url,
        channel_url,
        thumbnail_url,
        duration,
        filename as source_file

    from read_json(
        '{{ var("datalake_path") }}/output_*.json',
        format        = 'array',
        filename      = true,
        ignore_errors = true
    )

),

filtered as (

    select *
    from raw
    where title is not null
      and title <> ''

    -- On incremental runs skip files that are already loaded into the table.
    {% if is_incremental() %}
      and source_file not in (
          select distinct source_file
          from {{ this }}
      )
    {% endif %}

)

select
    md5(
        coalesce(title,        '') || '|' ||
        coalesce(channel,      '') || '|' ||
        coalesce(video_url,    '') || '|' ||
        coalesce(source_file,  '')
    )                   as video_sk,
    title,
    channel,
    views,
    upload_time,
    video_url,
    channel_url,
    thumbnail_url,
    duration,
    source_file,
    current_timestamp   as loaded_at

from filtered


