''' MiLoad: Load generator for MiVoter
    v0.01: Begin: generate sequential requests to server. Display basic metadata.
    v0.02: Issue N requests without waiting for responses. Display basic metadata. Single-core.
    v0.03: Store resulting metadata in pandas dataframe for later analysis. Single-core.
    v0.04: Display average and SD of times
    v0.05: Add command-line args: num_threads
    v0.06: Read user input from file
    v0.07: Isolate number of threads from number of URLs
    v0.08: Improve diagnostics
ToDo
    v0.09: Use input addresses, with street number low and high values, to randomize input.
    v0.10: Issue some requests without street numbers
    v0.12: Issue incorrect addresses (typos, wrong street numbers)
    v0.13: Isolate number of (simultaneous) threads from number of requests
    v0.20: Simulate user typing
    v0.30: Convert threading to multi-processing

    Author: Jeff Victor
'''
import argparse
from math import floor as floor
import pandas as pd
import random
import requests
import sys
import threading
import time

DEBUG=False

default_urls = [
    'https://address.mivoter.org/index.php?num=1&street=Main St',
    'https://address.mivoter.org/index.php?num=8000&street=anchor bay dr'
    ]

def issue_request(url, result, i):
    ''' Issue a web request to the specific URL. '''
    start    = time.time()
    response = requests.get(url)
    end      = time.time()
    elapsed  = f'{end-start:0.2f}'
    # Store data for later analysis.
    result['start'] = start
    result['end']   = end
    result['url']   = url[38:]
    result['ret_code'] = response
    result['elapsed']    = response.elapsed.total_seconds()
    if DEBUG:
        end_short = floor(end / 1000) * 1000
        print(f'{end-end_short:0.4f} : {url[38:]}: Found {response.json()["count"]} matches:',
              f'{response.json()["rows"][0]["low"]}',
              f'{response.json()["rows"][0]["high"]}',
              f'{response.json()["rows"][0]["street"]}')

def addr_to_url(address):
    num = ''
    prefix = 'https://address.mivoter.org/index.php?'
    address = address.strip()
    addr_list = address.split()
    if addr_list[0].isdigit():
        num = 'num=' + addr_list.pop(0) + '&'
    url = f'{prefix}{num}street={' '.join(addr_list)}'
    return url

def addr_range_to_url(row):
    prefix = 'https://address.mivoter.org/index.php?'
    street_num = random.randint(row['low'], row['high'])
    url = f'{prefix}num={street_num}&street={row["street"]}'
    return url

def main(num_threads, inputfile, rangefile):
    ''' main: retrieve optional address list, create threads, run all '''
    # Get addresses
    getaddr_start = time.time()
    if inputfile is None and rangefile is None:
        urls = default_urls
    elif inputfile is not None:
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
    else:
        range_df = pd.read_csv(rangefile)
        urls = range_df.apply(addr_range_to_url, axis=1).to_list()

    if DEBUG:
        print(f'Reading {len(urls)} addresses took {time.time()-getaddr_start:0.6f} seconds.')

    mkthreads_start = time.time()
    results = []
    # Create one empty dict for each thread
    for r in range(num_threads):
        results.append({})
    threads = []

    for i in range(int(num_threads)):
        url = urls[random.randint(0, len(urls)-1)]

        t = threading.Thread(target=issue_request, args=(url, results[i], i))
        threads.append(t)
    if DEBUG:
        print(f'Creating {num_threads} threads took {time.time()-mkthreads_start:0.6f} seconds.')

    all_start = time.time()
    for i in range(len(threads)):
        threads[i].start()

    for i in range(len(threads)):
        threads[i].join()
        res = results[i]
    all_end = time.time()
    all_elapsed = all_end - all_start

    results_df = pd.DataFrame(results)
    results_df_sort = results_df.sort_values(by='start')
    results_df_sort.to_csv('results.csv')

    print('\n==== Statistics ====')
    avg_time = results_df['elapsed'].mean()
    print(f'Executed {num_threads}, average: {avg_time:0.2f} sec, SD={results_df["elapsed"].std():0.3f}, Total elapsed time={all_elapsed:0.2f}, overall rate={num_threads/all_elapsed:0.2f}')

if __name__ == "__main__":
    pd.options.display.float_format = "{:.6f}".format
    
    parser = argparse.ArgumentParser(description="A sample script demonstrating argparse.")
    parser.add_argument('-t', '--threads', type=int, default=10, help="Number of simultaneous threads")
    parser.add_argument('-i', '--inputfile', type=str, default=None, help="File of addresses")
    parser.add_argument('-r', '--rangefile', type=str, default=None, help="File of address ranges")
    args = parser.parse_args()

    main(args.threads, args.inputfile, args.rangefile)


