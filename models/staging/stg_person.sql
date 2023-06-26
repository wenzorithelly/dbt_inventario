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
            json_extract_path_text(address, 'full_address') full_address,
            json_extract_path_text(address, 'city') city,
            case 
                when json_extract_path_text(address, 'state') like 'Rio de Janeiro' then 'RJ'
                when json_extract_path_text(address, 'state') like 'Distrito Federal' then 'DF'
                when json_extract_path_text(address, 'state') like 'Minas Gerais' then 'MG'
                when json_extract_path_text(address, 'state') like 'Paraná' then 'PR'
                when json_extract_path_text(address, 'state') like 'Bahia' then 'BA'
                when json_extract_path_text(address, 'state') like 'Alagoas' then 'AL' 
                when json_extract_path_text(address, 'state') like 'Amazonas' then 'AM'
                when json_extract_path_text(address, 'state') like 'Ceará' then 'CE'
                when json_extract_path_text(address, 'state') like 'Goiás' then 'GO'
                when json_extract_path_text(address, 'state') like 'Mato Grosso' then 'MT'
                when json_extract_path_text(address, 'state') like 'Mato Grosso do Sul' then 'MS'
                when json_extract_path_text(address, 'state') like 'Maranhão' then 'MA'
                when json_extract_path_text(address, 'state') like 'Pernambuco' then 'PE'
                when json_extract_path_text(address, 'state') like 'São Paulo' then 'SP'
                when json_extract_path_text(address, 'state') like 'Tocantins' then 'TO'
                when json_extract_path_text(address, 'state') like 'Rio Grande do Sul' then 'RS'
                when json_extract_path_text(address, 'state') like 'Sergipe' then 'SE'
                when json_extract_path_text(address, 'state') like 'Santa Catarina' then 'SC'
                when json_extract_path_text(address, 'state') like 'Pará' then 'PA'
                when json_extract_path_text(address, 'state') like 'Piauí' then 'PI'
                when json_extract_path_text(address, 'state') like 'Amapá' then 'AP'
                when json_extract_path_text(address, 'state') like 'Espírito Santo' then 'ES'
                when json_extract_path_text(address, 'state') like 'Roraima' then 'RR'
                when json_extract_path_text(address, 'state') like 'Brasília' then 'DF'
                when json_extract_path_text(address, 'state') like 'Rondônia' then 'RO'
                when json_extract_path_text(address, 'state') like 'Paraíba' then 'PB'
                when json_extract_path_text(address, 'state') like 'Rio Grande do Norte' then 'RN'
                when json_extract_path_text(address, 'state') like '' then null
                else json_extract_path_text(address, 'state')
            end as state
        from {{source('sources', 'person')}}
    ),

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

            -- address
            a.state,
            cast(a.zipcode as numeric) zipcode,
            a.full_address,
            a.city,

            -- description
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

            -- customers info
            cast(marital_status as text) marital_status,
            cast(occupation as text) occupation,
            case
                when length(regexp_replace(rg,'\D','','g')) = 0 then null
                else cast(regexp_replace(rg,'\D','','g') as numeric) end rg

        from {{source('sources', 'person')}} p
            left join cte on cte.client_id = p.id
            left join address a on a.client_id = p.id
            left join {{ref('description')}} d on d.id = p.id
        where rg not ilike all(array['%teste%', '%test%'])
            or name !~* 'teste' )

select
    *
from nominal