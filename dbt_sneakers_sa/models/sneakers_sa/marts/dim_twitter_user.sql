-- Set dbt incremental model configuration:
{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge'
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
            profile_id AS id,
            name AS twitter_real_name,
            username AS twitter_username,
            avatar AS twitter_avatar
        FROM stg_tweets
    )
SELECT *
FROM clean_data

{% if is_incremental() %}

WHERE TRUE

{% endif %}
