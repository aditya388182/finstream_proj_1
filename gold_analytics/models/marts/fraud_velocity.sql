{{ config(materialized='table') }}

WITH raw_swipes AS (
    -- We reference the source we just defined in the YAML file
    SELECT * FROM {{ source('silver_layer', 'transactions') }}
),

user_velocity AS (
    -- Aggregate the data to the user level
    SELECT 
        user_id,
        COUNT(transaction_id) AS swipe_count,
        SUM(amount) AS total_spend,
        MAX(event_time) AS latest_swipe
    FROM raw_swipes
    GROUP BY user_id
)

-- Flag the high-risk profiles
SELECT 
    user_id,
    swipe_count,
    total_spend,
    latest_swipe,
    CASE 
        WHEN swipe_count >= 3 THEN 'HIGH_VELOCITY_RISK'
        WHEN total_spend > 5000 THEN 'HIGH_VALUE_RISK'
        ELSE 'NORMAL'
    END AS risk_flag
FROM user_velocity
WHERE swipe_count >= 3 OR total_spend > 5000