import os, yaml, requests, time
import argparse
import numpy as np
from multiprocessing.pool import Pool
from dotenv import load_dotenv
from columnar import columnar
from click import style
from datetime import datetime

def parse_args():
    options = {}
    parser = argparse.ArgumentParser(description='Rockset Load Tester.')
    parser.add_argument('-c', '--config', help='yaml configuration file with test parameters', default='./resources/config.yaml')
    args = parser.parse_args()
    options['config_file'] = args.config

    return options

def load_config(options):
    config = {}

    # Get the test configuration file
    with open(options['config_file']) as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            exit("Could not process yaml config file")
    # Get the API KEY
    try:
        load_dotenv()
        apiKey = os.getenv('ROCKSET_APIKEY')
        config['target']['api_key'] = apiKey
    except KeyError as e:
         exit("Did not find ROCKSET_APIKEY defined in .env or environment")

    # Ensure the api server doesn't inclue the protocol
    api_server = config['target']['api_server']
    if api_server[0:8] == 'https://':
        replacement = api_server[8:]
        config['target']['api_server'] = replacement
    if api_server[0:7] == 'http://':
        replacement = api_server[7:]
        config['target']['api_server'] = replacement

    # Get information about the test environment. Also tests connectivity
    viResopnse = requests.get(
        'https://' + config['target']['api_server'] + '/v1/orgs/self/virtualinstances',
        headers={'Authorization': 'ApiKey ' + config['target']['api_key']})

    if viResopnse.status_code == 401:
        exit("Authorization failure connecting to target")
    if viResopnse.status_code != 200:
        exit(f'Unable to connect to target server. {viResopnse.reason}. {viResopnse.text}')
    viResponseJson = viResopnse.json()
    viResponseData = viResponseJson['data'][0]
    config['target']['vi_size'] = viResponseData['current_type']

    #Get information about the target org settings. These are only added to the output for reference
    orgResopnse = requests.get(
        'https://' + config['target']['api_server'] + '/v1/orgs/self/settings',
        headers={'Authorization': 'ApiKey ' + config['target']['api_key']})
    if orgResopnse.status_code != 200:
        exit(f'Unable to get org information from server. {viResopnse.reason}. {viResopnse.text}')
    orgResponseJson = orgResopnse.json()
    orgResponseData = orgResponseJson['data']

    config['target']['aggregator_parallelism'] = orgResponseData['aggregator_parallelism']
    config['target']['concurrent_queries_limit'] = orgResponseData['concurrent_queries_limit'] 
    config['target']['concurrent_query_execution_limit'] = orgResponseData['concurrent_query_execution_limit']

    # Add the test start time to the config
    stats = {}
    stats['test_start'] = datetime.now()
    config['stats'] = stats

    return config

def run_queryset(target, query_set):
    results = {}
    query_count = len(query_set['queries'])
    query_results = [None] * query_count
    with Pool(processes=len(query_set['queries'])) as pool:
        tasks = []
        for x in range(0, query_count):
            args = []
            args.append(x + 1)   # query_num
            args.append(0)       # user_num
            args.append(target)  # target
            args.append(query_set['queries'][x])  # query

            task = pool.apply_async(run_query, args)
            tasks.append(task)
        
        pool.close()
        pool.join()

        for task in tasks:
            result = task.get()
            offset = result['query_num']
            query_results[offset -1] = result
    
    results['query_results'] = query_results
    results['query_set_name'] = query_set['name']
    return results

def run_query(query_num, user_num, target, query):

    drop_results = False
    if 'overrides' in target:
        overrides = target['overrides']
        if overrides != None:
            if 'drop_results' in overrides:
                drop_results = True

    result = {}
    result['query_num'] = query_num
    if 'name' in query:
        result['name'] = query['name']
    else:
        result['name'] = '(unnamed)'

    # Run the query
    payload = {}
    
    if 'lambda' in query:

        qlURL = query['lambda']
        if qlURL[0] != '/':
            qlURL = '/' + qlURL

        if 'parameters' in query:
            payload['parameters'] = query['parameters']

        if drop_results:
            print('Warning: drop results is not currently supported when using query lambdas')

        start = time.perf_counter()
        qryResopnse = requests.post(
            'https://' + target['api_server'] + qlURL,
            json=payload,
            headers={'Authorization': 'ApiKey ' + target['api_key'] ,'Content-Type': 'application/json' })
        end = time.perf_counter()
    elif 'sql' in query:
        sql = {}
        sql['query'] = query['sql']
        if ('drop_results' in query and query['drop_results']) or drop_results:
            baseQuery = sql['query'].rstrip()
            if baseQuery[-1] == ';':
                baseQuery = baseQuery[:-1]
            sql['query'] = baseQuery + ' HINT(final_aggregator_drop_results=true)'
        if 'parameters' in query:
            sql['parameters'] = query['parameters']
        if 'paginate' in query:
            sql['paginate'] = query['paginate']
        if 'initial_paginate_response_doc_count' in query:       
            sql['initial_paginate_response_doc_count'] = query['initial_paginate_response_doc_count']

        payload['sql'] = sql

        start = time.perf_counter()
        qryResopnse = requests.post(
            'https://' + target['api_server'] + '/v1/orgs/self/queries',
            json=payload,
            headers={'Authorization': 'ApiKey ' + target['api_key'] ,'Content-Type': 'application/json' })
        end = time.perf_counter()
    else:
        result['status'] = 'invalid'
        result['message'] = 'Query definition has neither lambda or sql specified'
        return result

    if qryResopnse.status_code == 408:
        result['status'] = 'timeout'
    elif qryResopnse.status_code != 200:
        result['status'] = 'error'
        result['message'] = f'{qryResopnse.reason}. {qryResopnse.text}'
    else:
        result['status'] = 'success'
        result['round_trip_time_ms'] = round((end - start) * 1000)
        response_data = qryResopnse.json()
        result['server_time_ms'] = response_data['stats']['elapsed_time_ms']
        result['queued_time_ns'] = round(response_data['stats']['throttled_time_micros'])
        result['query_time_ms'] = round(result['server_time_ms'] - (result['queued_time_ns'] /1000))
        result['network_time_ms'] = result['round_trip_time_ms'] - result['server_time_ms']
        if drop_results:
          result['row_count'] = 'dropped'
        else:
          result['row_count'] = len(response_data['results'])

    return result

def obfuscate_apikey(config):
    # We obfuscate the apikey once we are done executing any queries to prevent it from being leaked in any reports
    last4 = config['target']['api_key'][-4:]
    config['target']['api_key'] = '******' + last4

def display_qs_results(config, results):

    # Display the individual query results
    headers = ['Query #', 'Name', 'Status', 'Total (ms)', 'Query (ms)', 'Queued (ms)', 'Network (ms)', 'Rows', 'Error']
    patterns = [
      ('error', lambda text: style(text, fg='red')),
      ('success', lambda text: style(text, fg='green')),
      ('timeout', lambda text: style(text, fg='yellow')),
    ]
    justify = ['l', 'l', 'c', 'r', 'r', 'r', 'r', 'r', 'l']
    data = []
    for result in results['query_results']:
        line = []
        line.append(result['query_num'])
        line.append(result['name'])
        line.append(result['status'])
        if result['status'] == 'success':
            line.append(result['round_trip_time_ms'])
            line.append(result['query_time_ms'])
            line.append(round(result['queued_time_ns']/1000))
            line.append(result['network_time_ms'])
            line.append(result['row_count'])
            line.append('')  # no error message
        elif result['status'] == 'error':
            line.extend(['','','','',''])  # blank values for timings
            line.append(result['message'])
        elif result['status'] == 'timeout':
            line.extend(['','','','',''])  # blank values for timings
            line.append('Query timed out')
        data.append(line)
    
    table = columnar(data, headers = headers, patterns = patterns, no_borders=True, justify=justify, preformatted_headers=True)
    print(table)

def summarize_qs_results(config, results):
    total, query, queued, network = 0,0,0,0 
    warnings = []
    for result in results['query_results']:
        if result['status'] == 'success':
            total += result['round_trip_time_ms']
            query += result['query_time_ms']
            queued += round(result['queued_time_ns']/1000)
            network += result['network_time_ms']
            if result['row_count'] == 0:
                warning = {}
                warning['query_num'] = result['query_num']
                warning['name'] = result['name']
                warning['message'] = 'Returned no rows'
                warnings.append(warning)
        elif result['status'] == 'error':
                warning = {}
                warning['query_num'] = result['query_num']
                warning['name'] = result['name']
                warning['message'] = f"Errored with message: {result['message']}"
                warnings.append(warning)
    
    return {
        'total': total,
        'query': query,
        'queued': queued,
        'network': network,
        'warnings': warnings
    }

def display_summary(config, summary):
    # Display the test parameters
    headers = ['Test', 'Started', 'VI', 'Agg Par', 'CQEL', 'CQL', 'Total (ms)', 'Query (ms)', 'Queued (ms)', 'Network (ms)']
    justify = ['l','c','c', 'r', 'r', 'r', 'r', 'r', 'r', 'r']
    data = [[
        config['test_name'],
        config['stats']['test_start'],
        config['target']['vi_size'], 
        config['target']['aggregator_parallelism'], 
        config['target']['concurrent_query_execution_limit'],
        config['target']['concurrent_queries_limit'],
        summary['total'],
        summary['query'],
        summary['queued'],
        summary['network']
        ]]
    table = columnar(data, headers = headers, justify = justify, no_borders=True, preformatted_headers=True)
    print(table)    

    if len(summary['warnings']) > 0:
        headers = ['Query #', 'Name', 'Status']
        data =[]
        for warning in summary['warnings']:
            line = []
            line.append(warning['query_num'])
            line.append(warning['name'])
            line.append(warning['message'])
            data.append(line)
        print("--- WARNINGS ---")
        table = columnar(data, headers = headers, no_borders=True, preformatted_headers=True)
        print(table)   


if __name__ == "__main__":
    options = parse_args()
    config = load_config(options)
    query_set_results = run_queryset(config['target'], config['query_set'][0])
    obfuscate_apikey(config)
    display_qs_results(config, query_set_results)
    summary = summarize_qs_results(config, query_set_results)
    display_summary(config, summary)