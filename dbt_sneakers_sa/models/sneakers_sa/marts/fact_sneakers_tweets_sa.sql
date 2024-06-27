-- Set dbt incremental model configuration:
{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

WITH
    stg_tweets AS (
        SELECT
            model,
            link,
            text,
            name,
            username,
            profile_id,
            avatar,
            date,
            "is-retweet",
            "is-pinned",
            comments,
            retweets,
            quotes,
            likes,
            tweet_sentiment
        FROM {{ ref('stg_raw__incremental_sneakers_twitter_sentiment_analysis') }}
    )
    
    , clean_data AS (
        SELECT DISTINCT
            -- get id from url: https://twitter.com/thibs21299/status/1320698095663943682#m using Trino SQL
            REGEXP_EXTRACT(link, 'status/([0-9]+)', 1) as id,
            model,
            text,
            profile_id AS twitter_user_id,
            -- Parse date string to timestamp keeping time: Oct 26, 2020 · 12:05 PM UTC using Trino SQL
            DATE_PARSE(REGEXP_REPLACE(date, ' UTC$', ''), '%b %d, %Y · %I:%i %p') AT TIME ZONE 'UTC' AS tweeted_at,
            "is-retweet",
            "is-pinned",
            comments,
            retweets,
            quotes,
            likes,
            tweet_sentiment
        FROM stg_tweets
    )
SELECT *
FROM clean_data

{% if is_incremental() %}

WHERE id NOT IN (SELECT id FROM {{ this }})

{% endif %}
