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
    v0.19: Make Sessions an option
    v0.20: Simulate users typing
    v0.21: Simulate users typing, phase 2
    v0.22: Report number of aborted connection requests, fix some minor bugs
    v0.23: Improve error handling
    v0.24: Count users per minute
    v0.25: Multiprocessing, Phase 1: make it work, no frills
    v0.26: Multiprocessing, Phase 2: consolidate output from proc's
    v0.27: Usability Phase 1: Use ssh to execute instances on other systems
ToDo
    v0.28: Forward cmdline options to remote systems
    v0.xx: Usability Phase 2: Aggregate output from remote instances
    v0.xx: Count connection aborts per minute
    v0.xx: Option to run on server: remove network latency factor from delay calculation
    v0.xx: Feedback loop to adjust delay
Later
    v0.xx: Total number of requests
    v0.xx: Issue incorrect addresses (typos, wrong street numbers)
    v0.xx: Isolate number of (simultaneous) threads from number of requests


    Author: Jeff Victor
'''
import argparse
import itertools
import json
from math import floor as floor
from multiprocessing import Process, Queue, set_start_method
import pandas as pd
import random
import re
import requests
import subprocess
import sys
import threading
import time

DEBUG=True
TYPING_DELAY = 0.03

default_urls = [
    'https://address.mivoter.org/index.php?num=1&street=Main St',
    'https://address.mivoter.org/index.php?num=8000&street=anchor bay dr'
    ]

def parse_host_file(file_path):
    ''' File is expected to be text, one entry per line, in the form:
        <user>@<host>. Passwordless ssh is required. '''
    with open(file_path, 'r') as file:
        lines = [line.strip() for line in file]
    return lines
 
    
def issue_request(session, url, result, i):
    ''' Issue a web request to the specific URL. '''
    start    = time.time()
    #######
    headers = requests.utils.default_headers()

    headers.update({
        'User-Agent': 'python-requests/2.31.0',
        'Accept-Encoding': 'gzip, deflate',
        'Accept': '*/*',
        'Connection': 'keep-alive',
    } )
    #######
    if session is None:
        try:
            response = requests.get(url, headers=headers)
        except requests.exceptions.ConnectionError:
#            print ('============ CONNECTION REQUEST ABORTED ============', flush=True)
             print ('A', flush=True, end='')
             return
    else:
        try:
            response = session.get(url)
        except requests.exceptions.ConnectionError:
#           Report CONNECTION REQUEST ABORTED : SESSION =====', flush=True)
            print ('As', flush=True, end='')
            return

    end = time.time()
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

        t = threading.Thread(target=issue_request, args=(None, url, results[i], i))
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

def one_user(url, results):
    ''' one_user(): Issue http requests that mimic a user typing sufficient keys to 
        find their address. Begin requests with 5 char's, increment. '''
    session = requests.Session()
    # Parse the URL so we can simulate typing
    begin = url[:44]
    patt = r"num=(?P<num>\w+)&street=(?P<street>[A-Za-z0-9 ]+)"
    match = re.search(patt, url)
    num_str = f"num={match.group('num')}"
    street_str = f"street={match.group('street')}"
    # print('.', end='', flush=True)
    street_len = len(match.group('street'))
    
    i = None
    end_at = min(street_len-3, 8)
    if end_at < 1:
        end_at = 1
    for i in range(end_at):
        addr_portion = f"{num_str}&street={street_str[7:11+i]}"
        typed_url = begin + addr_portion
        results.append({})
        issue_request(session, typed_url, results[i], i)
        time.sleep(.3)
    session.close()
    if i is None:
        print(f'one_user: i is unbound for {street_str}', flush=True)
    return i

def many_users(urls, delay, results, user_count, duration):
    ''' many_users(): simulate many users, searching for their address, 
        one at a time '''
    begin = time.time()
    if delay < 0:
        delay = 0
    for i in range(1000):
        url = urls[random.randint(0, len(urls)-1)]
        requests = one_user(url, results)
        if time.time() - begin > duration:
            break
        time.sleep(delay)
    # Store the number of users simulated.
    user_count.append(i+1)
        
    
def one_tub(urls, delay, results, duration, use_session):
    ''' one_tub(): For the Soak method, each thread issues a sequence of requests.'''
    begin = time.time()
    if use_session is True:
        session = requests.Session()
    else:
        session = None
    for i in range(1000):
        results.append({})
        url = urls[random.randint(0, len(urls)-1)]
        issue_request(session, url, results[i], i)
        if delay < 0:
            continue
        if time.time() - begin > duration:
            break
        time.sleep(delay)
    if use_session is True:
        session.close()

def soak(num_threads, urls, rate_goal, duration, use_session, user):
    ''' soak(): method issues a metered rate of requests to the server. '''
    # Create a list of lists for result info. Each of the individual lists 
    # will be shared with a thread so it can add result dicts to the list. 
    # The lists will be combined later and summarized.
    results_list_list = []
    # Create one empty list for each thread
    for r in range(num_threads):
        results_list_list.append([])
    # Create a list to store the number of users simulated by each thread.
    user_count_list_list = []
    for r in range(num_threads):
        user_count_list_list.append([])
    delay = num_threads/rate_goal - 0.04

    threads = []
    if user is False:
        for i in range(int(num_threads)):
            t = threading.Thread(target=one_tub, args=(urls, delay, results_list_list[i], duration, use_session))
            threads.append(t)
    else:
        for i in range(int(num_threads)):
            t = threading.Thread(target=many_users, args=(urls, delay, results_list_list[i], user_count_list_list[i], duration))
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
    total_users = 0
    for u_count in user_count_list_list:
        total_users += sum(u_count)
    return results, total_users, all_elapsed

def main(remote, num_threads, inputfile, rangefile, rate_goal, duration, use_session, user, qobj):
    ''' main: retrieve optional address list, create threads, run all '''
    # Get addresses
    total_users = 0
    if inputfile is None and rangefile is None:
        # Use the URLs defined above
        urls = default_urls
    elif inputfile is not None:
        # Use the inputfile as a list of addresses
        urls = read_static_addrs(inputfile)
    else:
        # rangefile is a list of street number ranges.
        # Use them to generate valid addreses.
        range_df = pd.read_csv(rangefile)
        prefix = 'https://address.mivoter.org/index.php?max=5&'
        range_df['url'] = prefix + 'num=' + range_df['low'].astype(str) + '&street=' + range_df['street']
        urls = range_df['url'].tolist()

    if rate_goal is not None:
        # method = 'Soak'
        delay = num_threads/rate_goal - 0.04
        if not remote:
            print(f'Set delay to {delay:0.2f}')
        results, total_users, all_elapsed = soak(num_threads, urls, rate_goal, duration, use_session, user)
    else:
        # method = 'Flood'
        results, all_elapsed = flood(num_threads, urls)

    results_df = pd.DataFrame(results)
    results_df_sort = results_df.sort_values(by='start')
    results_df_sort.to_csv('results.csv')

    avg_time = results_df['elapsed'].mean()
    user_rate = total_users/all_elapsed*60
    if not remote:
        print(f'Total users: {total_users}, {user_rate:0.0f} users per minute')
    
    # Send stats to spawner
    stats_dict = { 'result_count': len(results_df),
                   'elapsed': all_elapsed,
                   'user_rate': user_rate
    }
    qobj.put(stats_dict)

def thread_start_remote(args, host, cmdline, return_val):
    ''' Create a new process that uses ssh to start another 
        load instance on a remote computer. '''
    cmd_tokens = cmdline.split()
    stdout_err = subprocess.run(cmd_tokens, capture_output=True, text=True)
    stdout = stdout_err.stdout
    return_val.append(host+':::'+stdout)

def start_remote_instances(args, remote_returns_list_list):
    ''' Use ssh to start one instance on another computer, potentially with 
        multiple processes and/or threads. It will report its results in 
        JSON format. '''

    with open(args.nodes, 'r') as file:
        hosts = [line.strip() for line in file]
    num_hosts = len(hosts)

    # Prepare storage for output from remote instances.
    threads = []
    for i in range(num_hosts):
        print(f'Starting instance on {hosts[i]}.')
        cmdline = f'ssh {hosts[i]} /usr/bin/python3 MiLoad/miload.py -r -i MiLoad/addresses -s {args.soak} -t {args.threads} -d {args.duration}  -u  -p {args.processes}'
        t = threading.Thread(target=thread_start_remote, args=(args, hosts[i], cmdline, remote_returns_list_list[i]))
        threads.append(t)
    for i in range(num_hosts):
        threads[i].start()
    for i in range(len(threads)):
        threads[i].join()
    
    return remote_returns_list_list

def start_procs (args, empty):
    set_start_method('fork')
    procs = []
    queues = []
    stats = []
    for p in range(args.processes):
        qobj = Queue()
        queues.append(qobj)
        proc = Process(target=main, args=(args.remote,
            args.threads, args.inputfile, args.rangefile, 
            args.soak, args.duration, args.session, args.user, qobj))
        procs.append(proc)
        proc.start()
 
    for q in queues:
        stats.append(q.get())

    total_users = args.processes * args.threads
    total_results = 0
    elapsed_sum = 0
    user_rate_sum = 0
    for s in stats:
        total_results += s['result_count']
        elapsed_sum   += s['elapsed']
        user_rate_sum += s['user_rate']

    elapsed   = elapsed_sum / len(stats)

    if args.remote:
        remote_output = { 'user_rate': user_rate_sum }
        print(json.dumps(remote_output))

    else:
        print('=========\nOverall Statistics: ')
        print(f'Simultaneous users:   {int(total_users)}')
        print(f'Total results:   {int(total_results)}')
        print(f'Elapsed time:    {int(elapsed)}')
        print(f'Total user rate: {int(user_rate_sum)} per minute')

    for p in procs:
        p.join()

if __name__ == "__main__":
    pd.options.display.float_format = "{:.6f}".format
    
    parser = argparse.ArgumentParser(description="A sample script demonstrating argparse.")
    parser.add_argument('-d', '--duration', type=int, default=5, help="Duration of test")
    parser.add_argument('-t', '--threads', type=int, default=10, help="Number of simultaneous threads")
    parser.add_argument('-i', '--inputfile', type=str, default=None, help="File of addresses")
    parser.add_argument('-a', '--rangefile', type=str, default=None, help="File of address ranges")
    parser.add_argument('-s', '--soak', type=int, default=None, help="Desired request rate")
    parser.add_argument('-e', '--session', action='store_true', help="Use persistent session in a thread?")
    parser.add_argument('-u', '--user',  action='store_true', help="Simulate user typing?")
    parser.add_argument('-p', '--processes', type=int, default=1, help="Number of processes to run")
    parser.add_argument('-r', '--remote', action='store_true', help="I am a remotely controlled instance")
    parser.add_argument('-n', '--nodes', type=str, default='', help="File with list of <user>@<host> for remote instances")
    parser.add_argument('-o', '--output', type=str, default='text', help="Output format (text,json")



    args = parser.parse_args()
    if DEBUG and not args.remote:
        if args.soak is not None:
            print(f'Using Soak Method with desired rate goal: {args.soak}')
            if args.user:
                print('Using Soak Method with user simulation')
            else:
                print('Using Soak Method without users')
        else:
            print('Using flood method')
        print(f'Starting {args.processes} process(es)...')
        print(f'Starting {args.threads} threads per process...')
        
    # Prepare storage for output from remote instances.
    if not args.remote:
        with open(args.nodes, 'r') as file:
            hosts = [line.strip() for line in file]
        num_hosts = len(hosts)
        remote_returns_list_list = []
        for r in range(num_hosts):
            remote_returns_list_list.append([])

    thr_list = []
    if args.nodes:
        print(args.nodes)
        t = threading.Thread(target=start_remote_instances, args=(args, remote_returns_list_list))
        thr_list.append(t)
        t.start()
        
    t = threading.Thread(target=start_procs, args=(args, ''))
    thr_list.append(t)
    t.start()
    for i in range(2):
        thr_list[i].join()

    if args.nodes:
        for i in range(num_hosts):
            print(f'***{remote_returns_list_list[i]}')



