WITH
l AS (
    SELECT
        *
    FROM
        {{ ref('dim_listings') }}
),
h AS (
    SELECT * 
    FROM {{ ref('dim_hosts') }}
)

SELECT 
    l.listing_id,
    l.listing_name,
    l.room_type,
    l.minimum_nights,
    l.price,
    l.host_id,
    h.host_name,
    periods_of_stay,
    h.is_superhost as host_is_superhost,
    l.created_at,
    CAST(GREATEST(l.updated_at, h.updated_at) AS DATE) AS updated_at
FROM l
LEFT JOIN h ON (h.host_id = l.host_id)