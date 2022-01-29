import multiprocessing as mp
import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
import json
import re
import string
import unidecode

os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"


def de_emojify(text):
    regex_pattern = re.compile(pattern="["u"\U0001F600-\U0001F64F"  # emoticons
                                       u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                       u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                       u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                       "]+", flags=re.UNICODE)
    return regex_pattern.sub(r'', text)



os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"


def worker(msa_name, return_list, search_nearby):
    print("New Worker: {}".format(msa_name))
    result_count = 0

    url = "https://" + msa_name + ".craigslist.org/d/computer-gigs/search/cpg?query=python&is_paid=all"
    if search_nearby:
        url += "&searchNearby=1"

    content = BeautifulSoup(requests.get(url).text, 'html.parser')
    results = content.findAll("li", {"class": "result-row"})

    for result in results:
        return_list.append({'datetime': result.find("time")['datetime'],
                            'title': result.find("a", {"class": "result-title"}).text,
                            'link': result.find("a", {"class": "result-title"})['href']})
        result_count += 1

    print("{0} worker has finished: {1} results found".format(msa_name, 1))


def output():
    msas = {
        'edmonton': 1,
        'winnipeg': 1,
        'santabarbara': 1,
        'bellingham': 1,
        'santafe': 1,
        'ventura': 1,
        'bakersfield': 1,
        'vancouver': 1,
        'gulfport': 1,
        'omaha': 1,
        'victoria': 1,
        'atlanta': 1,
        'tulsa': 1,
        'tippecanoe': 1,
        'cincinnati': 1,
        'saltlakecity': 1,
        'austin': 1,
        'boston': 1,
        'palmsprings': 1,
        'chicago': 1,
        'springfieldil': 1,
        'dallas': 1,
        'denver': 1,
        'detroit': 1,
        'houston': 1,
        'lynchburg': 1,
        'lasvegas': 1,
        'losangeles': 1,
        'miami': 1,
        'minneapolis': 1,
        'newyork': 1,
        'wichita': 1,
        'philadelphia': 1,
        'pittsburgh': 1,
        'phoenix': 1,
        'portland': 1,
        'boulder': 1,
        'merced': 1,
        'annarbor': 1,
        'gainesville': 1,
        'seattle': 1,
        'sfbay': 1,
        'lincoln': 1,
        'tucson': 1,
        'southbend': 1,
        'neworleans': 1,
        'collegestation': 1,
        'lansing': 1,
        'raleigh': 1,
        'boise': 1,
        'reno': 1,
        'humboldt': 1,
        'orangecounty': 1,
        'ithaca': 1,
        'rochester': 1,
        'washingtondc': 1
    }
    manager = mp.Manager()
    return_list = manager.list()
    jobs = []

    for msa in msas:
        p = mp.Process(target=worker, args=(msa, return_list, msas[msa]))
        jobs.append(p)
        p.start()

    for proc in jobs:
        proc.join()

    df = pd.DataFrame(sorted(list(return_list), key=lambda i: i['datetime'], reverse=True))
    df = df.drop_duplicates(subset=['title'])

    return df.to_dict(orient='records')


def lambda_handler(event, context):
    t_response = output()

    response_object = {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(t_response)
    }

    return t_response


if __name__ == '__main__':
    search_results = output()
    with open('programming_gigs.json', 'w') as f:
        json.dump(search_results, f, indent=4, sort_keys=True)


