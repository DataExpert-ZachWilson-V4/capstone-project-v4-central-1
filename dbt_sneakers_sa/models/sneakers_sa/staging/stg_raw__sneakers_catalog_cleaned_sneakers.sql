
with source as (

    select * from {{ source('raw', 'sneakers_catalog_cleaned_sneakers') }}

),

renamed as (

    select
        id,
        brand,
        model,
        gender,
        color,
        date_added,
        sneaker_url,
        sneaker_image,
        error,
        url,
        properties_error,
        properties_url

    from source

)

select * from renamed

