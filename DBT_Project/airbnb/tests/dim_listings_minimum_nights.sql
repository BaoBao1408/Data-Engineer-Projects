select * from {{ ref('dim_listings_cleaned') }}
where minimum_nights < 1
limit 10