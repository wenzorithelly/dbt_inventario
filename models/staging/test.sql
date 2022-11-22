select
    json_extract_path_text(address, 'state')
from {{source('sources', 'person')}}