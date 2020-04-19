import multiprocessing as mp
import random as rd
import time
import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
import json

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
        'atlanta': 0,
        'austin': 0,
        'boston': 0,
        'chicago': 0,
        'dallas': 0,
        'denver': 0,
        'detroit': 0,
        'houston': 0,
        'lasvegas': 0,
        'losangeles': 1,
        'miami': 0,
        'minneapolis': 0,
        'newyork': 0,
        'philadelphia': 0,
        'phoenix': 0,
        'portland': 0,
        'seattle': 0,
        'sfbay': 1,
        'washingtondc': 0
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
