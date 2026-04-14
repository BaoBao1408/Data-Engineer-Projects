{{ config(materialized='table') }}
select * from {{ ref('silver_fact_sale') }}