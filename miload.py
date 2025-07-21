''' MiLoad: Load generator for MiVoter
    v0.01: Begin: generate sequential requests to server. Display basic metadata.
    v0.02: Issue N requests without waiting for responses. Display basic metadata. Single-core.
    v0.03: Store resulting metadata in pandas dataframe for later analysis. Single-core.
    v0.04: Display average and SD of times
    v0.05: Add command-line args: num_threads
ToDo
    v0.06: Read user input from file
    v0.07: Convert threading to multi-processing
    v0.20: Simulate user typing

    Author: Jeff Victor
'''
import argparse
import pandas as pd
import random
import requests
import threading
import time

urls = [
    'https://address.mivoter.org/index.php?num=1&street=Main St',
    'https://address.mivoter.org/index.php?num=8000&street=anchor bay dr'
    ]

def issue_request(url, result, i):
    ''' Issue a web request to the specific URL. '''
    start    = time.time()
    response = requests.get(url)
    end      = time.time()
    elapsed  = f'{end-start:0.2f}'
    result['start'] = start
    result['end']   = end
    result['url']   = url[38:]
    result['ret_code'] = response
    result['elapsed']    = response.elapsed.total_seconds()


def main(num_threads):
    results = []
    for r in range(num_threads):
        results.append({})
    threads = []

    for i in range(int(num_threads)):
        url = urls[random.randint(0, 1)]

        t = threading.Thread(target=issue_request, args=(url, results[i], i))
        threads.append(t)

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
    args = parser.parse_args()

    main(args.threads)


