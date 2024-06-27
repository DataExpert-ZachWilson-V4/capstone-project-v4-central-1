{% snapshot scd_twitter_user %}

{{
    config(
      target_schema='jsgomez14',
      unique_key='id',
      strategy='check',
      check_cols=['twitter_real_name', 'twitter_username', 'twitter_avatar']
    )
}}

WITH dim_twitter_user AS (
    SELECT
        id,
        twitter_real_name,
        twitter_username,
        twitter_avatar
    FROM {{ ref('dim_twitter_user') }}
)
SELECT *
FROM dim_twitter_user

{% endsnapshot %}