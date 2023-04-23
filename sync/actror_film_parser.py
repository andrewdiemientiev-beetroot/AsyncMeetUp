import time

import wikipediaapi
import requests
from bs4 import BeautifulSoup
import re


def search_actors_by_year_of_birth(year):
    # Search List of actors with Academy Awards
    academy_awards_page = wikipediaapi.Wikipedia('en').page('List of actors with Academy Award nominations')
    academy_awards_soup = BeautifulSoup(requests.get(academy_awards_page.fullurl).text, 'html.parser')
    academy_awards_table = academy_awards_soup.find('table', {'class': 'sortable wikitable'})

    # Extract names and year of birth from table
    actors = {}
    for row in academy_awards_table.find_all('tr')[1:]:
        cells = row.find_all('td')
        if len(cells) >= 4:
            name = cells[0].get_text().strip()
            year_pattern = r'\d{4}'
            year_of_birth = re.findall(year_pattern, cells[2].get_text().strip())
            if year_of_birth and year_of_birth[0] == year:
                actor_page = wikipediaapi.Wikipedia('en').page(cells[0].find('a').get('href')[6:])
                if actor_page.exists():
                    actors[name] = actor_page

    return actors


def get_film_history(actor, actor_page):
    print(actor_page)
    actor_soup = BeautifulSoup(requests.get(actor_page.fullurl).text, 'html.parser')
    film_table = actor_soup.find('table', class_=re.compile('wikitable sortable'))
    if film_table:
        films = []
        for row in film_table.find_all('tr')[1:]:
            cells = row.find_all('td')
            if cells:
                title = cells[0].get_text().strip()
                year = cells[1].get_text().strip()
                films.append((title, year))
        # Write films to file
        with open('{}_film_history.txt'.format(actor), 'w') as f:
            for title, year in films:
                f.write('{} ({})\n'.format(title, year))
    else:
        print('Could not find film history table for {}'.format(actor))


def main():
    year = '1970'

    # Search for actors with specified year of birth
    actors = search_actors_by_year_of_birth(year)

    print(actors)
    # Retrieve film history for each actor and write it to a file
    for actor, link in actors.items():
        get_film_history(actor, link)


if __name__ == '__main__':
    start = time.time()
    main()
    print(f"Finished in: {time.time() - start}")