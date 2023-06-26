{{config(materialized='table', sort='timestamp', dist='user_id')}}

select 
    id,
    name,
    created_at,
    email,
    phone,
    cpf,
    rg,
    state,
    zipcode,
    full_address,
    city,
    occupation,
    marital_status
from {{ref('stg_person')}}
