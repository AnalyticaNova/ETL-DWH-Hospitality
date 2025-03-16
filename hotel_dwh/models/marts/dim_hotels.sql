SELECT DISTINCT
    hotel AS hotel_name
FROM {{ ref('stg_bookings') }}
