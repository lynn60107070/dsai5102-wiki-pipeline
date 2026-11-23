-- Dimension: one row per wiki (grain: wiki_id).
-- Project and language come from the server name (en.wikipedia.org -> wikipedia, en), so every wiki
-- in the facts has a row even before the sitematrix seed (dim_wiki_names) is filled; the seed
-- adds the language names where it has them.

with seen as (

    select wiki_id, any_value(server_name) as server_name
    from {{ ref('stg_edits') }}
    where {{ before_run_start() }}
    group by wiki_id
    {% if var("pageviews_enabled") %}
    union all
    -- wikis that only appear in the pageview files (always Wikipedias)
    select distinct wiki_id,
           replace(regexp_replace(wiki_id, 'wiki$', ''), '_', '-') || '.wikipedia.org' as server_name
    from {{ ref('stg_wiki_views') }}
    {% endif %}

),

one_per_wiki as (

    select wiki_id, any_value(server_name) as server_name from seen group by wiki_id

),

parsed as (

    select
        wiki_id,
        server_name,
        split_part(server_name, '.', 1)                  as subdomain,
        split_part(server_name, '.', -2)                 as project_domain
    from one_per_wiki

),

parsed_with_fallbacks as (

    select
        *,
        case
            when server_name = 'www.wikidata.org'   then 'wikidata'
            when project_domain = 'wikimedia'        then subdomain        -- commons, meta, species, ...
            else project_domain                                            -- wikipedia, wiktionary, ...
        end                                                                as parsed_family,
        case when project_domain in ('wikipedia', 'wiktionary', 'wikisource', 'wikibooks', 'wikinews',
                                     'wikiquote', 'wikiversity', 'wikivoyage')
             then subdomain end                                            as parsed_language_code
    from parsed

)

-- The seed (sitematrix) wins wherever it has a value; the parsed values cover wikis it doesn't list.
select
    p.wiki_id,
    p.server_name,
    coalesce(n.project_family, p.parsed_family)                            as project_family,
    coalesce(n.project_family, p.parsed_family) = 'wikipedia'              as is_wikipedia,
    coalesce(n.language_code, p.parsed_language_code)                      as language_code,
    coalesce(n.language_name, p.parsed_language_code)                      as language_name,
    n.local_language_name,
    coalesce(n.is_arabic, p.parsed_language_code in ('ar', 'arz', 'ary'))  as is_arabic
from parsed_with_fallbacks as p
left join {{ ref('dim_wiki_names') }} as n using (wiki_id)
