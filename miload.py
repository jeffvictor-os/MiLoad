''' MiLoad: Load generator for MiVoter
    v0.01: Begin: generate sequential requests to server. Display basic metadata.
    v0.02: Issue N requests without waiting for responses. Display basic metadata. Single-core.
    v0.03: Store resulting metadata in pandas dataframe for later analysis. Single-core.
    v0.04: Display average and SD of times
    v0.05: Add command-line args: num_threads
    v0.06: Read user input from file
    v0.07: Isolate number of threads from number of URLs
    v0.08: Improve diagnostics
    v0.09: Use input addresses, with street number low and high values, to randomize input.
    v0.10: Issue some requests without street numbers
    v0.11: Fix street number selection: odd, even, both
    v0.12: Remove randomized street numbers
    v0.13: Refactor main() to prep for adding soak
    v0.14: Add soak method, single-threaded
    v0.15: Multithread the soak method
    v0.16: Set maximum number of returned addresses to 5
    v0.17: Option to set loadtest duration
    v0.18: Use a Session for URL requests
ToDo
    v0.19: Option to run on server: remove network latency factor from delay calculation
Later
    v0.20: Total number of requests
    v0.21: Feedback loop to adjust delay
    v0.xx: Issue incorrect addresses (typos, wrong street numbers)
    v0.xx: Isolate number of (simultaneous) threads from number of requests
    v0.xx: Simulate user typing
    v0.xx: Convert threading to multi-processing

    Author: Jeff Victor
'''
import argparse
import itertools
from math import floor as floor
import pandas as pd
import random
import requests
import sys
import threading
import time

DEBUG=True

default_urls = [
    'https://address.mivoter.org/index.php?num=1&street=Main St',
    'https://address.mivoter.org/index.php?num=8000&street=anchor bay dr'
    ]

def issue_request(session, url, result, i):
    ''' Issue a web request to the specific URL. '''
    start    = time.time()
    response = session.get(url)
    end      = time.time()
    # Store data for later analysis.
    result['start'] = start
    result['end']   = end
    result['url']   = url[38:]
    result['ret_code'] = response
    result['elapsed']    = response.elapsed.total_seconds()
    resp_json = response.json()
    result['num_matches'] = len(resp_json['rows'])
    if DEBUG == 2:
        end_short = floor(end / 1000) * 1000
        print(f'{end-end_short:0.4f} : {url[38:]}: Found {response.json()["count"]} matches:',
              f'{response.json()["rows"][0]["low"]}',
              f'{response.json()["rows"][0]["high"]}',
              f'{response.json()["rows"][0]["street"]}')

def addr_to_url(address):
    ''' Assumes that every address begins with a street number. '''
    num = ''
    prefix = 'https://address.mivoter.org/index.php?max=5&'
    address = address.strip()
    addr_list = address.split()
    if addr_list[0].isdigit():
        num = 'num=' + addr_list.pop(0) + '&'
    url = f'{prefix}{num}street={" ".join(addr_list)}'
    return url

def read_static_addrs(inputfile):
    try:
        with open(inputfile, 'r') as file:
            lines = file.readlines()
            urls = [addr_to_url(line) for line in lines]

    except FileNotFoundError:
        print(f'Error: The file "{inputfile}" was not found.')
        sys.exit(1)
    except Exception as e:
        print(f'An error occurred: {e}')
        sys.exit(1)

    return urls

def flood(num_threads, urls):
    results = []
    # Create one empty dict for each thread
    for r in range(num_threads):
        results.append({})
    threads = []

    mkthreads_start = time.time()
    for i in range(int(num_threads)):
        url = urls[random.randint(0, len(urls)-1)]

        t = threading.Thread(target=issue_request, args=(url, results[i], i))
        threads.append(t)
    if DEBUG:
        print(f'Creating {num_threads} threads took {time.time()-mkthreads_start:0.6f} seconds.')

    all_start = time.time()
    for i in range(len(threads)):
        threads[i].start()
        time.sleep(0.009)

    for i in range(len(threads)):
        threads[i].join()

    all_end = time.time()
    all_elapsed = all_end - all_start

    return results, all_elapsed

def one_tub(urls, delay, results, duration):
    ''' For the Soak method, each thread issues a sequence of requests.'''
    begin = time.time()
    session = requests.Session()
    for i in range(1000):
        results.append({})
        url = urls[random.randint(0, len(urls)-1)]
        issue_request(session, url, results[i], i)
        if delay < 0:
            continue
        if time.time() - begin > duration:
            break
        time.sleep(delay)

def soak(num_threads, urls, rate_goal, duration):
    ''' This method issues a metered rate of requests to the server. '''
    # Create list of lists. Each of the lists will be shared with a thread so
    # it can add result dicts to the list. The lists will be combined later.
    results_list_list = []
    # Create one empty list for each thread
    for r in range(num_threads):
        results_list_list.append([])
    delay = num_threads/rate_goal - 0.04
    if DEBUG:
        print(f'Delay: {delay:0.3f}')

    threads = []
    for i in range(int(num_threads)):
        t = threading.Thread(target=one_tub, args=(urls, delay, results_list_list[i], duration))
        threads.append(t)

    all_start = time.time()
    for i in range(len(threads)):
        threads[i].start()
        time.sleep(0.009)

    for i in range(len(threads)):
        threads[i].join()

    all_end = time.time()
    all_elapsed = all_end - all_start

    results = list(itertools.chain(*results_list_list)) 
    return results, all_elapsed

def main(num_threads, inputfile, rangefile, rate_goal, duration):
    ''' main: retrieve optional address list, create threads, run all '''
    # Get addresses
    getaddr_start = time.time()
    if inputfile is None and rangefile is None:
        # Use the URLs defined above
        urls = default_urls
    elif inputfile is not None:
        # Use the inputfile as a list of addresses
        urls = read_static_addrs(inputfile)
        if DEBUG:
            print(f'Reading {len(urls)} addresses took {time.time()-getaddr_start:0.6f} seconds.')
    else:
        # rangefile is a list of street number ranges.
        # Use them to generate valid addreses.
        range_df = pd.read_csv(rangefile)
        prefix = 'https://address.mivoter.org/index.php?max=5&'
        range_df['url'] = prefix + 'num=' + range_df['low'].astype(str) + '&street=' + range_df['street']
        urls = range_df['url'].tolist()

    if rate_goal is not None:
        method = 'Soak'
        print('Rate goal=', rate_goal)
        results, all_elapsed = soak(num_threads, urls, rate_goal, duration)
    else:
        method = 'Flood'
        results, all_elapsed = flood(num_threads, urls)

    results_df = pd.DataFrame(results)
    results_df_sort = results_df.sort_values(by='start')
    results_df_sort.to_csv('results.csv')

    print('\n==== Statistics ====')
    avg_time = results_df['elapsed'].mean()
    
    print(f'Inputs: {method} method, rate goal={rate_goal}, threads={num_threads}')
    print(f'{len(results_df)} requests, average: {avg_time:0.3f} sec, SD={results_df["elapsed"].std():0.3f}, Total elapsed time={all_elapsed:0.2f}, overall rate={len(results)/all_elapsed:0.2f}')
    print(f'Match distribution: min: {results_df["num_matches"].min()}, max: {results_df["num_matches"].max()}, avg: {results_df["num_matches"].mean():0.2f}\n')

if __name__ == "__main__":
    pd.options.display.float_format = "{:.6f}".format
    
    parser = argparse.ArgumentParser(description="A sample script demonstrating argparse.")
    parser.add_argument('-d', '--duration', type=int, default=5, help="Duration of test")
    parser.add_argument('-t', '--threads', type=int, default=10, help="Number of simultaneous threads")
    parser.add_argument('-i', '--inputfile', type=str, default=None, help="File of addresses")
    parser.add_argument('-r', '--rangefile', type=str, default=None, help="File of address ranges")
    parser.add_argument('-s', '--soak', type=int, default=None, help="Desired request rate")
    args = parser.parse_args()

    main(args.threads, args.inputfile, args.rangefile, args.soak, args.duration)


