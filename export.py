import json
from database_handler import DatabaseHandler

export_data = {
    'name': 'Sites',
    'children': []
}

database_handler = DatabaseHandler(1, 1)

sites = database_handler.fetch_all_sites()

for site in sites:

    site_pages = database_handler.fetch_pages_by_site(site[0])

    child_pages = []

    for page in site_pages:
        child_pages.append(
            {
                'name': page[3],
                'type': page[2],
                'size': 5,
                'children': []
            }
        )

    export_data['children'].append(
        {
            'name': site[1],
            'children': child_pages
        }
    )


print("EXPORT DATA: ", export_data)

with open('data.json', 'w') as outfile:
    json.dump(export_data, outfile)