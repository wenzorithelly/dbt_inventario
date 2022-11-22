with
    nominal as (
        select
            id,
            split_part(description, '||', 1) childs_under_18,
            split_part(description, '||', 2)::text disagreement_between_parties,
            split_part(description, '||', 3) testment,
            split_part(description, '||', 4) time_to_start,
            split_part(description, '||', 5) inventory_started,
            split_part(description, '||', 6) decision_maker,
            split_part(description, '||', 7) resources,
            split_part(description, '||', 8) payment
        from {{source('sources', 'person')}}

    ),
    final as (
        select
            id,
            case when childs_under_18 like '%Falecido%' then string_to_array(childs_under_18, ' ') end childs_under_18,
            case when disagreement_between_parties like '%Discordância%' then string_to_array(disagreement_between_parties, ' ') end disagreement_between_parties,
            case when testment like '%Testamento%' then string_to_array(testment, ' ') end testment,
            case when time_to_start like '%quanto tempo%' then string_to_array(time_to_start, ' ') end time_to_start,
            case when inventory_started like '%iniciou%' then string_to_array(inventory_started, ' ') end inventory_started,
            case when decision_maker like '%tomador%' then string_to_array(decision_maker, ' ') end decision_maker,
            case when resources like '%patrimônio%' then string_to_array(resources, ' ') end resources,
            case when payment like '%arcar%' then string_to_array(payment, ' ') end payment
        from nominal
    
    )

select 
    id,
    childs_under_18[9] childs_under_18,
    disagreement_between_parties[9] disagreement_between_parties,
    testment[6] testment,
    time_to_start[11] time_to_start,
    inventory_started[8] inventory_started,
    decision_maker[10] decision_maker,
    resources[9] resources,
    payment[10] payment
from final