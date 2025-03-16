SELECT DISTINCT
    country AS customer_country,
    is_repeated_guest
FROM {{ ref('stg_bookings') }}
