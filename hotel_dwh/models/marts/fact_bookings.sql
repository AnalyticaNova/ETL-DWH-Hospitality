{{ config(materialized='table') }}


SELECT
    booking_id,
    hotel,
    is_canceled,
    lead_time,
    arrival_date,
    booking_date,
    adr,
    market_segment,
    distribution_channel,
    is_repeated_guest,
    total_of_special_requests
FROM staging.stg_bookings
