{{config(materialized='table', sort='timestamp', dist='user_id')}}

select 
    name,
    created_at,
    email,
    phone,
    cpf,
    rg,
    active,
    address,
    state,
    occupation,
    marital_status
from {{ref('person')}}
