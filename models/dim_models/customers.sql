{{config(materialized='table', sort='timestamp', dist='user_id')}}

select
    id,
    name,
    state,
    childs_under_18,
    disagreement_between_parties,
    testment,
    time_to_start,
    inventory_started,
    decision_maker,
    resources,
    payment,
    deceased
from {{ref('person')}}