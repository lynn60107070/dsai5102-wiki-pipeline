-- One row per edit or page creation from the raw zone, typed and de-duplicated.
-- The raw sink is at-least-once, so the same event can appear twice; meta.id is Wikimedia's event id.
-- The column list is fixed here (schema-on-read with a declared contract), so a new or missing field
-- in the stream can never silently change the table.
-- User names are not stored: only a hash (to count distinct editors) and the editor type.

with source as (

    select *
    from read_json(
        's3://{{ var("lake_bucket") }}/raw/edits/*/*/*.jsonl.gz',
        format = 'newline_delimited',
        hive_partitioning = true,
        columns = {
            meta: 'STRUCT(id VARCHAR, dt VARCHAR, domain VARCHAR)',
            id: 'BIGINT',
            type: 'VARCHAR',
            namespace: 'INTEGER',
            title: 'VARCHAR',
            comment: 'VARCHAR',
            timestamp: 'BIGINT',
            "user": 'VARCHAR',
            bot: 'BOOLEAN',
            minor: 'BOOLEAN',
            length: 'STRUCT(old BIGINT, new BIGINT)',
            revision: 'STRUCT(old BIGINT, new BIGINT)',
            server_name: 'VARCHAR',
            wiki: 'VARCHAR'
        }
    )

),

deduplicated as (

    select *
    from source
    qualify row_number() over (partition by meta.id order by "timestamp") = 1

)

select
    meta.id                                                        as event_id,
    id                                                             as recent_change_id,
    revision.new                                                   as revision_id,
    to_timestamp("timestamp")                                      as event_ts,
    date_trunc('hour', to_timestamp("timestamp"))                  as event_hour,
    wiki                                                           as wiki_id,
    server_name,
    namespace,
    title,
    type = 'new'                                                   as is_new_page,
    bot                                                            as is_bot,
    coalesce(minor, false)                                         as is_minor,
    case
        when bot then 'bot'
        -- temporary accounts (~2025-...) and IP addresses are logged-out editors
        when "user" like '~%'
          or regexp_matches("user", '^\d{1,3}(\.\d{1,3}){3}$')
          or regexp_matches("user", '^[0-9A-Fa-f:]+:[0-9A-Fa-f:]+$') then 'anonymous'
        else 'registered'
    end                                                            as editor_type,
    md5("user")                                                    as editor_key,
    coalesce(length.new, 0) - coalesce(length.old, 0)              as bytes_changed,
    -- heuristic: edit summaries that undo or revert someone else's change
    regexp_matches(lower(coalesce(comment, '')),
                   'undid revision|\bundo\b|\brevert|\brv\b|rollback')  as is_revert
from deduplicated
