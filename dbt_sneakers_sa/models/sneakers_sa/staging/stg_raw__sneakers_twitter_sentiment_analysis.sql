
with source as (

    select * from {{ source('raw', 'sneakers_twitter_sentiment_analysis') }}

),

renamed as (

    select
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

    from source

)

select * from renamed

