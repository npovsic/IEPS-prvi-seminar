import json
from database_handler import DatabaseHandler

# EXPORT DATA FOR PACK VISUALISATION SITES AND PAGES
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


#print("EXPORT DATA: ", export_data)

with open('data.json', 'w') as outfile:
    json.dump(export_data, outfile)

# EXPORT DATA FOR NETWORK VISUALISATION CONNECTIONS BETWEEN PAGES

export_links_data = {
    'links': []
}

links = database_handler.fetch_links_from_specific_site()

for link in links:
    export_links_data['links'].append(
        {
            'source': link[2],
            'target': link[3],
            'weight': 1,
            'type': 'licencing'
        }
    )

print("EXPORT LINKS: ", export_links_data)

with open('links_data.json', 'w') as links_outfile:
    json.dump(export_links_data, links_outfile)
