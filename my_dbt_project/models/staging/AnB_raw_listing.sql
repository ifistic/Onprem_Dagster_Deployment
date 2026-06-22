WITH raw_listing AS (
    SELECT * FROM {{ source('airbnb', 'listings') }}
)

SELECT
    id AS listing_id,
    name AS listing_name,
    listing_url,
    room_type,
    minimum_nights,
    host_id,
    TO_NUMBER(REPLACE(REPLACE(price, '$', ''), ',', '')) AS price,
    CAST(created_at AS DATE) AS created_date,
    CAST(updated_at AS DATE) AS updated_date,
    DATEDIFF('day', CAST(created_at AS DATE), CAST(updated_at AS DATE)) AS days_between_created_and_updated,
    DATE_PART('year', CURRENT_DATE) - DATE_PART('year', CAST(created_at AS DATE)) AS period_of_creation_years
FROM raw_listing

