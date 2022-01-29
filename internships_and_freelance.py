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
import ssl
from urllib3 import exceptions
ssl._create_default_https_context = ssl._create_unverified_context


os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"
TESTING_MODE = False
seven_days_ago = datetime.datetime.now() - datetime.timedelta(7)
seven_days_ago = seven_days_ago.replace(hour=0, minute=0, second=0, microsecond=0)

six_days_ago = seven_days_ago




def clean_punctuation(title_str, convert_to_lowercase=False):
    if convert_to_lowercase:
        title_str = title_str.lower()

    title_str = title_str.replace('-', ' ')
    title_str = title_str.replace('$', ' ')
    title_str = title_str.replace('/', ' ')
    title_str = title_str.replace('.', ' ')
    title_str = title_str.replace('\u2665', ' ')
    title_str = title_str.replace('\u2013', '-')


    title_str = unidecode.unidecode(title_str)
    title_str = title_str.translate(str.maketrans('', '', string.punctuation))

    return title_str


searches = ["intern", "contract", "contractor", "analyst"]


keywords = ["python", "sql"]


bad_links_csv_url = 'https://raw.githubusercontent.com/ivan-sepulveda/prometheus_insights/master/bad_links.csv'
bad_links_df = pd.read_csv(bad_links_csv_url, sep=",")
bad_links = bad_links_df['link'].to_list()

bad_titles_csv_url = 'https://raw.githubusercontent.com/ivan-sepulveda/prometheus_insights/master/bad_titles.csv'
bad_titles_df = pd.read_csv(bad_titles_csv_url, sep=",")
bad_titles = bad_titles_df['title'].to_list()

#bad_keywords_csv_url = 'https://raw.githubusercontent.com/ivan-sepulveda/prometheus_insights/master/bad_keywords.csv'
#bad_keywords_df = pd.read_csv(bad_keywords_csv_url, sep=",")
#quick_filters = bad_keywords_df['keyword'].to_list()
quick_filters = []

msas = {'newyork': 0}

if not TESTING_MODE:
    msas_csv_url = 'https://raw.githubusercontent.com/ivan-sepulveda/prometheus_insights/master/msas.csv'
    msas_df = pd.read_csv(msas_csv_url, sep=",")
    msas_df_records = msas_df.to_dict('records')
    msas = {record['msa']: record['search'] for record in msas_df.to_dict('records')}



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
    time.sleep(1)
    for search in searches:
        q = search if (" " not in search) else search.replace(" ", "+")

        gigs_url = "https://" + msa_name + ".craigslist.org/search/ggg?query=" + q + "&is_paid=all"
        jobs_url = "https://" + msa_name + ".craigslist.org/search/jjj?query=" + q + "&is_paid=all"

        time.sleep(2)
        for url in [gigs_url, jobs_url]:
            time.sleep(3)

            #stub_len = len("https://")
            #adapter = url[-1*stub_len:]
            #print("adapter: {0}".format(adapter))

            #session = requests.Session()
            #session.config['keep_alive'] = False
            #retry = Retry(connect=3, backoff_factor=0.5)
            #adapter = HTTPAdapter(max_retries=retry)
            #session.mount('http://', adapter)
            #session.mount('https://', adapter)
            #session.get(url)
            #requests.adapters.DEFAULT_RETRIES = 7

            try:
                time.sleep(3)
                content = BeautifulSoup(requests.get(url).text, 'html.parser')
                results = content.findAll("li", {"class": "result-row"})
                skip = False
            except requests.exceptions.ConnectionError:
                print("ConnectionError for {0}".format(msa_name))
                continue
            except exceptions.NewConnectionError:
                print("ConnectionError for {0}".format(msa_name))
                continue

            for result in results:
                post_date_as_str = result.find("time")['datetime'] + ":00"
                post_date_as_dt = datetime.datetime.strptime(post_date_as_str, '%Y-%m-%d %H:%M:%S')

                if post_date_as_dt < seven_days_ago:
                    continue

                clean_title = de_emojify(result.find("a", {"class": "result-title"}).text)
                clean_title = clean_title.lower()

                if result.find("a", {"class": "result-title"})['href'] in bad_links \
                        or result.find("a", {"class": "result-title"}).text in bad_titles:
                    continue
                else:
                    for quick_filter in quick_filters:
                        if quick_filter in clean_title:
                            skip = True
                            break
                if skip:
                    continue

                clean_title = clean_punctuation(clean_title)

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

    print("\t{0} worker has finished: {1} results found".format(msa_name, result_count))
    time.sleep(1.5)



def output():
    manager = mp.Manager()
    return_list = manager.list()
    jobs = []
    job_count = 0
    print("Starting msas jobs\n")
    for msa in msas:
        if job_count > 0  and job_count % 5 == 0:
            time.sleep(5)
        p = mp.Process(target=worker, args=(msa, return_list, msas[msa]))
        jobs.append(p)
        p.start()
        job_count += 1

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

    with open('goods2.json', 'w') as f:
        json.dump(goods, f, indent=4, sort_keys=True)

    with open('bads2.json', 'w') as f:
        json.dump(bads, f, indent=4, sort_keys=True)

    t1 = time.time()

    total = t1 - t0
    print("Total Time: ", total)