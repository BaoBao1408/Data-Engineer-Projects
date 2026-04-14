{{ config(materialized='table') }}
select * from {{ ref('silver_dim_store') }}