{{config(materialized='table', sort='timestamp', dist='user_id')}}

with
    cte as (
        select
            id client_id,
            string_to_array(description, ' ') as match_description
        from {{source('sources', 'person')}}
    ),
    address as (
        select
            id client_id,
            json_extract_path_text(address, 'zipcode') zipcode,
            json_extract_path_text(address, 'state') state

    )

    nominal as (
        select 
            p.id,
            cast(created_at as date) created_at,
            cast(name as text) name,
            cast(email as text) email,
            case
                when length(regexp_replace(phone,'\D','','g')) = 0 then null
                else cast(regexp_replace(phone,'\D','','g') as numeric) end phone,
            case
                when length(regexp_replace(cpf,'\D','','g')) = 0 then null
                else cast(regexp_replace(cpf,'\D','','g') as numeric) 
            end cpf,
            active,
            password,
            address,
            s.state_updated as state,
            cast(campaign as text) campaign,
            d.childs_under_18,
            d.disagreement_between_parties,
            d.testment,
            d.time_to_start,
            d.inventory_started,
            d.decision_maker,
            d.resources,
            d.payment,
            cast(deceased as text) deceased,
            labels,
            cast(marital_status as text) marital_status,
            cast(occupation as text) occupation,
            case
                when length(regexp_replace(rg,'\D','','g')) = 0 then null
                else cast(regexp_replace(rg,'\D','','g') as numeric) end rg
        from {{source('sources', 'person')}} p
            left join cte on cte.client_id = p.id
            left join {{ref('states')}} s on s.id = p.id
            left join {{ref('description')}} d on d.id = p.id
        where rg not ilike all(array['%teste%', '%test%'])
            or name !~* 'teste' )

select
    *
from nominal