
SELECT
    RAW_DATA:c::FLOAT AS current_price,
    RAW_DATA:d::FLOAT AS change_amount,
    RAW_DATA:dp::FLOAT AS change_percent,
    RAW_DATA:h::FLOAT AS day_high,
    RAW_DATA:l::FLOAT AS day_low,
    RAW_DATA:o::FLOAT AS day_open,
    RAW_DATA:pc::FLOAT AS prev_close,

    TO_TIMESTAMP_NTZ(RAW_DATA:t::NUMBER) AS market_timestamp,

    RAW_DATA:symbol::STRING AS symbol,

    TO_TIMESTAMP_NTZ(RAW_DATA:fetched_at::NUMBER) AS fetched_at,

    LOADED_AT
FROM {{ source('raw', 'bronze_stocks_raw') }}