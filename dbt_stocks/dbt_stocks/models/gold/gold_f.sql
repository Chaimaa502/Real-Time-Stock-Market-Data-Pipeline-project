
WITH ranked_quotes AS (

    SELECT
        symbol,
        current_price,
        change_amount,
        change_percent,
        day_high,
        day_low,
        day_open,
        prev_close,
        market_timestamp,
        fetched_at,

        ROW_NUMBER() OVER (
            PARTITION BY symbol
            ORDER BY fetched_at DESC
        ) AS rn

    FROM {{ ref('silver') }}

)

SELECT
    symbol,
    current_price,
    change_amount,
    change_percent,
    day_high,
    day_low,
    day_open,
    prev_close,
    market_timestamp,
    fetched_at

FROM ranked_quotes
WHERE rn = 1