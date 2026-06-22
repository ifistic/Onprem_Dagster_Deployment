WITH src_listings AS (
  SELECT
    *
  FROM
    {{ ref('AnB_raw_listing') }}
)
SELECT
  listing_id,
  listing_name,
  room_type,
  CASE
    WHEN minimum_nights = 0 THEN 1
    ELSE minimum_nights
  END AS minimum_nights,
  host_id,
  price,
  created_date AS created_at,
  period_of_creation_years AS periods_of_stay,
  updated_date AS updated_at
FROM
  src_listings
