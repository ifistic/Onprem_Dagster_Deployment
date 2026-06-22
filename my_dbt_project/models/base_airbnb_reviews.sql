with source as (
        select * from {{ source('airbnb', 'reviews') }}
  ),
  renamed as (
      select
          {{ adapter.quote("LISTING_ID") }},
        {{ adapter.quote("DATE") }},
        {{ adapter.quote("REVIEWER_NAME") }},
        {{ adapter.quote("COMMENTS") }},
        {{ adapter.quote("SENTIMENT") }}

      from source
  )
  select * from renamed
    