{{ config(materialized='table') }}

SELECT
    booking_id,
    hotel,
    is_canceled,
    lead_time,
    (arrival_date_year || '-' || LPAD(arrival_date_month::TEXT, 2, '0') || 
'-' || LPAD(arrival_date_day_of_month::TEXT, 2, '0'))::DATE AS 
arrival_date,
    (arrival_date_year || '-' || LPAD(arrival_date_month::TEXT, 2, '0') || 
'-' || LPAD(arrival_date_day_of_month::TEXT, 2, '0'))::DATE - lead_time AS 
booking_date,
    stays_in_weekend_nights,
    stays_in_week_nights,
    adults,
    children,
    babies,
    meal,
    country,
    market_segment,
    distribution_channel,
    is_repeated_guest,
    previous_cancellations,
    previous_bookings_not_canceled,
    reserved_room_type,
    assigned_room_type,
    booking_changes,
    deposit_type,
    agent,
    days_in_waiting_list,
    customer_type,
    adr,
    required_car_parking_spaces,
    total_of_special_requests,
    reservation_status,
    reservation_status_date
FROM {{ source('staging', 'hotel_bookings') }}
