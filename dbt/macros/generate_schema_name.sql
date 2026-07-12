-- Override dbt's default schema naming behaviour.
--
-- By default dbt generates schema names as: <target_schema>_<model_schema>
-- e.g. gold_silver, gold_gold — which creates nested databases.
--
-- This macro resolves the schema directly to the configured custom schema name
-- (e.g. 'silver' or 'gold'), matching our medallion database names.

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
