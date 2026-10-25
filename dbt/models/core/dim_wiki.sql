-- Dimension: one row per wiki (grain: wiki_id).
-- Project and language come from the server name (en.wikipedia.org -> wikipedia, en), so every wiki
-- in the facts has a row even before the sitematrix seed (dim_wiki_names, task S3) is filled; the seed
-- adds the language names where it has them.

with seen as (

    select wiki_id, any_value(server_name) as server_name
    from {{ ref('stg_edits') }}
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

)

select
    p.wiki_id,
    p.server_name,
    case
        when p.server_name = 'www.wikidata.org'        then 'wikidata'
        when p.project_domain = 'wikimedia'             then p.subdomain        -- commons, meta, species, ...
        else p.project_domain                                                  -- wikipedia, wiktionary, ...
    end                                                                        as project_family,
    p.project_domain = 'wikipedia'                                             as is_wikipedia,
    case when p.project_domain in ('wikipedia', 'wiktionary', 'wikisource', 'wikibooks', 'wikinews',
                                   'wikiquote', 'wikiversity', 'wikivoyage')
         then p.subdomain end                                                  as language_code,
    coalesce(n.language_name,
             case when p.project_domain in ('wikipedia', 'wiktionary', 'wikisource', 'wikibooks',
                                            'wikinews', 'wikiquote', 'wikiversity', 'wikivoyage')
                  then p.subdomain end)                                        as language_name,
    n.local_language_name,
    coalesce(n.is_arabic, p.subdomain = 'ar')                                  as is_arabic
from parsed as p
left join {{ ref('dim_wiki_names') }} as n using (wiki_id)
