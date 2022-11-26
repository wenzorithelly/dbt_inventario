{{config(materialized='table', sort='timestamp', dist='user_id')}}

select
    id,
    created_at,
    name,
    email,
    phone,
    cpf,
    rg,
    active,
    state,
    campaign, 
    childs_under_18, 
    disagreement_between_parties,
    testment,
    time_to_start, 
    inventory_started,
    decision_maker, 
    resources,
    payment,
    deceased, 
    marital_status, 
    occupation
from {{ref('stg_person')}}
