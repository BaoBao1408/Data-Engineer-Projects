

select 1 as error_rows
from {{ ref('fct_reviews') }} r
inner join {{ ref('dim_listings_cleaned') }} l
    on r.listing_id = l.listing_id
where r.review_date < l.created_at