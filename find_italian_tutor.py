import multiprocessing as mp
import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
import json
import re
import string
import unidecode
import time
import datetime

os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"
TESTING_MODE = False
seven_days_ago = datetime.datetime.now() - datetime.timedelta(7)
seven_days_ago = seven_days_ago.replace(hour=0, minute=0, second=0, microsecond=0)

searches = ["tutor", "homework"]

keywords = ["java", "python", "sql", "vba", "macros", "code", "coding", "math", "mathematics", "science", "sat",
            "calc", "calculus", "data" "common core", "physics", "geometry", "trigonometry", "trig", "essay",
            "essays", "programming", "biology", "chemistry", "chem", "bio", "ochem", "biochem", "biochemistry",
            "spanish", "analytics", "lsat", "psat"]

bad_links = [
    "https://boulder.craigslist.org/edu/d/boulder-math-and-reading-language-arts/7244286212.html",
    "https://sfbay.craigslist.org/nby/hea/d/healdsburg-bilingual-spanish-ma-full/7232811593.html",
    "https://ventura.craigslist.org/edu/d/ventura-early-childhood-educator-iii/7247718926.html",
    "https://sfbay.craigslist.org/sby/sof/d/san-jose-full-stack-developer-java-type/7247715462.html",
    "https://abbotsford.craigslist.org/edu/d/abbotsford-east-tutor-or-respite/7244147700.html"
]

bad_titles = [
    "Ivy League graduates sought to edit college application essays- $30/hr",
    "Handyman Wanted! $20-$35/hr. Make your own schedule",
    "Earn Up To $17/hr - Be Your Own Boss - DoorDash Driver",
    "Tutor the LSAT From Home - $60 an Hour!",
    "Looking for potential business partner(s) that knows coding...",
    "Looking for a partner to start new business (C#, SQL Server)",
    "$$ to match us up with design/programming students for our program",
    "Customer Service Writer/Advisor-Tues-Sat Shift",
    "Jobs now available for bid in Orange County- Tulbelt.com",
    "Foreign Currency Trader - Finance, Part Time, Remote - Work From Home",
    "Make $60,000+ and Get PAID to Learn"
]

quick_filters = ["lyft", "uber", "doordash", "tulbelt", "driver", "model", "drivers", "cleaner", "cleaning", "pca",
                 "sex", "handyman", "assistant", "assistants", "manager", "caregiver", "artist", "furniture", "song",
                 "construction", "tax", "fedex", "teacher", "fellowship", "truck", "movers", "barista", "agent",
                 "agents", "warehouse", "nanny", "paint", "boxes", "architect", 'paralegal', "diabet", "secretar",
                 "mechanic", "moving", "volunteer", "partner", "delivery", "merchandiser", "electrician", "plumber",
                 "engineer", "eggs", "realtor", "officer", "snagajob", "substitute", "hvac", "casting", "foreman",
                 "concrete", "limpiadores", "singer", "musicians", "receptionist", "blog", "anthology", "woman", "park",
                 "accounting", "sperm", "housekeeper", "ultrasound", "personnel", "market", "property", "covid",
                 "instructor", "lights", "care", "admin", "interviewer", "seamstres", "apprentice", "nurse", "shovel",
                 "illustrator", "technician", "bullshit", "operator", "lockdown", "training", "groundskeeper",
                 "condoms", "welder", "garage", "coronavirus", "athlet", "laundry", "nursery", "custodian", "archivist",
                 "actress", "agency", "airbrush", "creative", "office", "keeper", "videographer", "event", "cresco",
                 "video", "cook", "ventas", "insurance", "restaurant", "celebrity", "laborers", "billing", "counselor",
                 "coordinator", "cna", "unlicensed", "collector", "harvest", "caribbean", "county", "auto"]

def de_emojify(text):
    regex_pattern = re.compile(pattern="["u"\U0001F600-\U0001F64F"  # emoticons
                                       u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                       u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                       u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                       "]+", flags=re.UNICODE)
    return regex_pattern.sub(r'', text)


def worker(msa_name, return_list, search_nearby):
    print("New Worker: {}".format(msa_name))
    result_count = 0

    for search in searches:
        q = search if (" " not in search) else search.replace(" ", "+")

        gigs_url = "https://" + msa_name + ".craigslist.org/search/ggg?query=" + q + "&is_paid=all"
        jobs_url = "https://" + msa_name + ".craigslist.org/search/jjj?query=" + q + "&is_paid=all"

        if search_nearby:
            gigs_url += "&searchNearby=1"

        for url in [gigs_url, jobs_url]:
            content = BeautifulSoup(requests.get(url).text, 'html.parser')
            results = content.findAll("li", {"class": "result-row"})
            skip = False

            for result in results:
                post_date_as_str = result.find("time")['datetime'] + ":00"
                post_date_as_dt = datetime.datetime.strptime(post_date_as_str, '%Y-%m-%d %H:%M:%S')

                if post_date_as_dt < seven_days_ago:
                    continue

                clean_title = de_emojify(result.find("a", {"class": "result-title"}).text)
                clean_title = clean_title.lower()

                if result.find("a", {"class": "result-title"})['href'] in bad_links \
                        or result.find("a", {"class": "result-title"}).text in bad_titles:
                    skip = True
                else:
                    for quick_filter in quick_filters:
                        if quick_filter in clean_title:
                            skip = True
                            break
                if skip:
                    continue

                clean_title = clean_title.replace('-', ' ')
                clean_title = clean_title.replace('$', ' ')
                clean_title = clean_title.replace('/', ' ')
                clean_title = clean_title.replace('.', ' ')

                clean_title = unidecode.unidecode(clean_title)
                clean_title = clean_title.translate(str.maketrans('', '', string.punctuation))

                clean_title_list_words = [word.strip() for word in clean_title.split()]

                keyword_match = False

                for keyword in keywords:
                    if keyword in clean_title_list_words:
                        keyword_match = True
                        break

                current_post = {
                    'datetime': post_date_as_str,
                    'title': result.find("a", {"class": "result-title"}).text,
                    'link': result.find("a", {"class": "result-title"})['href']
                }

                if keyword_match:
                    current_post["added"] = 1
                    result_count += 1
                else:
                    current_post["added"] = 0

                return_list.append(current_post)

    print("{0} worker has finished: {1} results found".format(msa_name, result_count))


def output():
    test_msas = {
        'newyork': 0
    }

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
    t0 = time.time()

    search_results = output()
    goods, bads = list(), list()

    for post in search_results:
        if post["added"] == 1:
            goods.append(
                {key: post[key] for key in post if key != "added"}
            )
        else:
            bads.append(
                {key: post[key] for key in post if key != "added"}
            )

    with open('it_goods.json', 'w') as f:
        json.dump(goods, f, indent=4, sort_keys=True)

    with open('it_bads.json', 'w') as f:
        json.dump(bads, f, indent=4, sort_keys=True)


    t1 = time.time()

    total = t1 - t0
    print("Total Time: ", total)