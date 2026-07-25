{% macro cineflux_deterministic_hash(columns) -%}
to_hex(
    sha256(
        to_utf8(
            concat_ws(
                '||',
                {%- for column in columns %}
                coalesce(cast({{ column }} as varchar), '__null__'){{ ',' if not loop.last }}
                {%- endfor %}
            )
        )
    )
)
{%- endmacro %}
