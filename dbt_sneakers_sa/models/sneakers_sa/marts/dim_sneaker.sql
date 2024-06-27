WITH
    stg_tweets AS (
        SELECT
            id,
            brand,
            model,
            gender,
            color,
            date_added,
            sneaker_url,
            sneaker_image
        FROM {{ ref('stg_raw__sneakers_catalog_cleaned_sneakers') }}
        WHERE "error" IS NULL AND properties_error IS NULL
    )
    
SELECT 
    *,
    brand || ' ' || model AS join_key
FROM stg_tweets