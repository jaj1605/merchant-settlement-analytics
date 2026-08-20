{#
    Shared casting helpers.

    The raw layer is all VARCHAR by design, so every staging model performs the same
    two conversions. Centralising them means the definition of "an amount" or "a
    cleaned string" lives in exactly one place — change it here and every model
    inherits the change.
#}

{% macro to_amount(column) %}
    {#- Money: empty string and the literal 'NULL' both become NULL, not 0.
        Coercing missing money to zero silently understates variances, which is
        exactly the class of bug this pipeline exists to catch. -#}
    try_cast(nullif(nullif(trim({{ column }}), ''), 'NULL') as decimal(18, 2))
{% endmacro %}


{% macro clean_string(column) %}
    {#- Text: trim, and treat the literal string 'NULL' as a real NULL. The export
        writes 'NULL' as text rather than leaving the field empty. -#}
    nullif(nullif(trim({{ column }}), ''), 'NULL')
{% endmacro %}
