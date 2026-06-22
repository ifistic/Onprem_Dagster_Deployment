{{ 
  config(
    materialized = 'incremental',
    on_schema_change = 'fail'
  ) 
}}

WITH src_reviews AS (
  SELECT * FROM {{ ref('src_reviews_model') }}
)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['listing_id', 'review_date', 'reviewer_name', 'review_text']) }} AS review_id,
  * 
FROM src_reviews
WHERE review_text IS NOT NULL

{% if is_incremental() %}
  {% if var("start_date", False) and var("end_date", False) %}
    AND review_date >= '{{ var("start_date") }}'
    AND review_date < '{{ var("end_date") }}'
    {{ log('Loading ' ~ this ~ ' incrementally (start_date: ' ~ var("start_date") ~ ', end_date: ' ~ var("end_date") ~ ')', info=True) }}
  {% else %}
    AND review_date > (SELECT MAX(review_date) FROM {{ this }})
    {{ log('Loading ' ~ this ~ ' incrementally (using MAX review_date)', info=True) }}
  {% endif %}
{% endif %}
