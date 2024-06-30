WITH agg_stg AS (
  SELECT
    FCT.id AS tweet_id,
    U.id AS user_id,
    S.id AS sneaker_id,
    S.brand,
    S.model,
    CAST(YEAR(FCT.tweeted_at) AS VARCHAR) AS year_tweeted,
    FCT."is-retweet" AS is_retweet,
    FCT."is-pinned" AS is_pinned,
    FCT.comments,
    FCT.retweets,
    FCT.quotes,
    FCT.likes,
    CASE
        WHEN FCT.tweet_sentiment BETWEEN -0.1 AND 0.1 THEN 'Neutral'
        WHEN FCT.tweet_sentiment > 0.1 THEN 'Positive'
        WHEN FCT.tweet_sentiment < -0.1 THEN 'Negative'
    END bucketized_sentiment
  FROM {{ ref('fact_sneakers_tweets_sa') }} AS FCT
  LEFT JOIN {{ ref('dim_twitter_user') }} AS U ON U.id = FCT.twitter_user_id
  LEFT JOIN {{ ref('dim_sneaker') }} AS S ON S.join_key = FCT.model
)
SELECT CASE 
            WHEN GROUPING(brand, bucketized_sentiment, year_tweeted) = 0 THEN 'brand_sentiment_year'
            WHEN GROUPING(brand, bucketized_sentiment) = 0 THEN 'brand_sentiment'
            WHEN GROUPING(model, bucketized_sentiment) = 0 THEN 'model_sentiment'
        END as aggregation_level,
        COALESCE(brand, 'Overall') AS brand,
        COALESCE(model, 'Overall') AS model,
        COALESCE(year_tweeted, 'Overall') AS year_tweeted,
        COALESCE(bucketized_sentiment, 'Overall') AS sentiment,
        SUM(IF(is_pinned,1,0)) AS total_pinned,
        SUM(comments) AS total_comments,
        SUM(retweets) AS total_retweets,
        SUM(likes) AS total_likes,
        COUNT(CASE WHEN bucketized_sentiment = 'Positive' THEN 1 END) AS total_positive,
        COUNT(CASE WHEN bucketized_sentiment = 'Neutral' THEN 1 END) AS total_neutral,
        COUNT(CASE WHEN bucketized_sentiment = 'Negative' THEN 1 END) AS total_negative
FROM agg_stg
GROUP BY GROUPING SETS (
  (brand, bucketized_sentiment, year_tweeted),
  (brand, bucketized_sentiment),
  (model, bucketized_sentiment)
)