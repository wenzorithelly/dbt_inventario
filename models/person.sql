with
    cte as (
        select
            id client_id,
            string_to_array(description, ' ') as match_description
        from {{source('sources', 'person')}}
    ),

    nominal as (
        select 
            id,
            cast(created_at as date) created_at,
            name,
            email,
            regexp_replace(phone,'\D','','g') phone,
            active,
            password,
            address,
            campaign,
            regexp_replace(cpf,'\D','','g') cpf,
            match_description[9] deceased_childs_under_18,
            match_description[18] disagreement_between_parties,
            match_description[24] testment,
            match_description[35] time_to_start,
            match_description[43] inventory_started,
            match_description[53] decision_maker,
            match_description[62] resources,
            match_description[72] payment,
            deceased,
            labels,
            marital_status,
            occupation,
            regexp_replace(rg,'\D','','g') rg
        from {{source('sources', 'person')}} p
            left join cte on cte.client_id = p.id
        where rg not ilike all(array['%teste%', '%test%'])
            or name !~* 'teste' )

select * from nominal