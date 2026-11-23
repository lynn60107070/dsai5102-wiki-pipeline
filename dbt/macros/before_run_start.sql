{#
  Only edits from hours that ended before this dbt run started.

  fact_edit, dim_article and dim_wiki each read the live clean zone. The clean sinks keep writing the
  current hour while a run is in progress, so a file landing between two of these models could reach
  fact_edit but not dim_article, and the relationship test then failed (4 runs on 8-10 Nov). Every model stops at the same hour, taken
  from dbt's run_started_at, so the hour still being written is left out of all of them and loaded by
  the next run. Late files for older hours could still land mid-run, but those are rare (replays).
#}
{% macro before_run_start(column='event_ts') -%}
    {{ column }} < timestamp '{{ run_started_at.strftime("%Y-%m-%d %H:00:00") }}'
{%- endmacro %}
