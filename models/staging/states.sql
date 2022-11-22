with
    nominal as (
        select
            id,
            json_extract_path_text(address, 'state') state
        from {{source('sources', 'person')}}
    )

select
    id,
    case 
        when state like 'Rio de Janeiro' then 'RJ'
        when state like 'Distrito Federal' then 'DF'
        when state like 'Minas Gerais' then 'MG'
        when state like 'Paraná' then 'PR'
        when state like 'Bahia' then 'BA'
        when state like 'Alagoas' then 'AL' 
        when state like 'Amazonas' then 'AM'
        when state like 'Ceará' then 'CE'
        when state like 'Goiás' then 'GO'
        when state like 'Mato Grosso' then 'MT'
        when state like 'Mato Grosso do Sul' then 'MS'
        when state like 'Maranhão' then 'MA'
        when state like 'Pernambuco' then 'PE'
        when state like 'São Paulo' then 'SP'
        when state like 'Tocantins' then 'TO'
        when state like 'Rio Grande do Sul' then 'RS'
        when state like 'Sergipe' then 'SE'
        when state like 'Santa Catarina' then 'SC'
        when state like 'Pará' then 'PR'
        when state like 'Piauí' then 'PI'
        when state like 'Amapá' then 'AP'
        when state like 'Espírito Santo' then 'ES'
        when state like 'Roraima' then 'RR'
        when state like 'Brasília' then 'DF'
        when state like 'Rondônia' then 'RO'
        when state like 'Paraíba' then 'PB'
        when state like 'Rio Grande do Norte' then 'RS'
        when state like '' then null
        else state
    end as state_updated
from nominal