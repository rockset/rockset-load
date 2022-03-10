import os, yaml, requests, uuid, csv
import argparse

from executors import ParallelQSExecutor, SerialQSExecutor
from dotenv import load_dotenv
from columnar import columnar
from click import style
from datetime import datetime

def parse_args():
    options = {}
    parser = argparse.ArgumentParser(description='Rockset Load Tester.')
    parser.add_argument('-v', '--verbose', help='print information to the screen', action="store_true")
    parser.add_argument('--nolog', help='suppresses output log', action="store_true")
    parser.add_argument('-c', '--config', help='yaml configuration file with test parameters', default='./resources/config.yaml')
    parser.add_argument('-o', '--output_dir', help='directory where output is writen', default='./history')

    args = parser.parse_args()
    options['config_file'] = args.config
    options['verbose'] = args.verbose
    options['output_dir'] = args.output_dir

    # TODO Make the history files configurable
    options['history_dir'] = './history'
    options['details_name'] = 'query_details.csv'
    options['qs_summary_name'] = 'query_set_summaries.csv'
    options['log_output'] = not args.nolog
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

    stats = {}
 
   # Add the test start time  and test id to the config stats
    stats['test_start'] = datetime.now()
    stats['run_id'] = uuid.uuid4()
    config['stats'] = stats

    # Ensure the test name is a valid string
    if not 'test_name' in config:
        config['test_name'] = 'unnamed'

    return config

def run_queryset(target, query_set):

    if 'execution_mode' in target:
        mode = target['execution_mode']
    else:
        mode = 'serial'

    if mode == 'parallel':
        executor =  ParallelQSExecutor(target, query_set)
    elif mode == 'serial':
        executor =  SerialQSExecutor(target, query_set)
    else:
        print(f"Unexpected query set execution mode {mode}")
        return None

    return executor.run()

def obfuscate_apikey(config):
    # We obfuscate the apikey once we are done executing any queries to prevent it from being leaked in any reports
    last4 = config['target']['api_key'][-4:]
    config['target']['api_key'] = '******' + last4

def display_qs_results(config, results):

    # Display the individual query results
    headers = ['Test','Q Set Name', 'Q Set #', 'Status', 'Total (ms)', 'Query (ms)', 'Queued (ms)', 'Network (ms)', 'Rows', 'Error']
    patterns = [
      ('error', lambda text: style(text, fg='red')),
      ('success', lambda text: style(text, fg='green')),
      ('timeout', lambda text: style(text, fg='yellow')),
    ]
    justify = ['l', 'l', 'c', 'r', 'r', 'r', 'r', 'r', 'l']
    data = []
    for result in results['query_results']:
        line = []
        line.append(config['test_name'])
        line.append(result['name'])
        line.append(result['query_num'])
        line.append(result['status'])
        if result['status'] == 'success':
            line.append(result['round_trip_ms'])
            line.append(result['query_ms'])
            line.append(round(result['queued_ns']/1000))
            line.append(result['network_ms'])
            line.append(result['row_count'])
            line.append('')  # no error message
        elif result['status'] == 'error':
            line.extend(['','','','',''])  # blank values for timings
            line.append(result['message'])
        elif result['status'] == 'timeout':
            line.extend(['','','','',''])  # blank values for timings
            line.append('Query timed out')
        elif result['status'] == 'exhausted':
            line.extend(['','','','',''])  # blank values for timings
            line.append('Resources exhausted')
        data.append(line)
    
    table = columnar(data, headers = headers, patterns = patterns, no_borders=True, justify=justify, preformatted_headers=True)
    print(table)

def summarize_qs_results(config, results):
    total, query, queued, network = 0,0,0,0 
    warnings = []
    clean = True
    for result in results['query_results']:
        if result['status'] == 'success':
            total += result['round_trip_ms']
            query += result['query_ms']
            queued += round(result['queued_ns']/1000)
            network += result['network_ms']
            if result['row_count'] == 0:
                clean = False
                warning = {}
                warning['query_num'] = result['query_num']
                warning['name'] = result['name']
                warning['message'] = 'Returned no rows'
                warnings.append(warning)
        elif result['status'] == 'error':
                clean = False
                warning = {}
                warning['query_num'] = result['query_num']
                warning['name'] = result['name']
                warning['message'] = f"Errored with message: {result['message']}"
                warnings.append(warning)
        elif result['status'] == 'timeout':
                clean = False
                warning = {}
                warning['query_num'] = result['query_num']
                warning['name'] = result['name']
                warning['message'] = 'Query timed out'
                warnings.append(warning)
        elif result['status'] == 'exhausted':
                clean = False
                warning = {}
                warning['query_num'] = result['query_num']
                warning['name'] = result['name']
                warning['message'] = 'Resources exhausted'
                warnings.append(warning)
    
    return {
        'total_ms': total,
        'query_ms': query,
        'queued_ms': queued,
        'network_ms': network,
        'warnings': warnings,
        'clean': clean
    }

def display_qs_summary(config, summary):
    # Display the test parameters
    headers = ['Test', 'Started', 'Clean', 'Total (ms)', 'Query (ms)', 'Queued (ms)', 'Network (ms)', 'VI', 'Agg Par', 'CQEL', 'CQL']
    justify = ['l', 'r', 'r', 'r', 'r', 'r', 'r', 'r']
    patterns = [
      ('False', lambda text: style(text, fg='red')),
      ('True', lambda text: style(text, fg='green'))
    ]
    data = [[
        config['test_name'],
        config['stats']['test_start'],
        summary['clean'],
        summary['total_ms'],
        summary['query_ms'],
        summary['queued_ms'],
        summary['network_ms'],
        config['target']['vi_size'], 
        config['target']['aggregator_parallelism'], 
        config['target']['concurrent_query_execution_limit'],
        config['target']['concurrent_queries_limit']
        ]]
    table = columnar(data, headers = headers, patterns = patterns, justify = justify, no_borders=True, preformatted_headers=True)
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

def validate_history_dir(options):
    output_dir = options['output_dir']
    # Make sure the details file exists
    history_exists = os.path.exists(output_dir)
    if not history_exists:
        os.makedirs(output_dir)

def log_query_results(options, config, query_results):
    validate_history_dir(options)
    output_dir = options['output_dir']
    # Make sure the history files exists
    file_name = options['details_name']
    file_path = output_dir + '/' + file_name
    file_exists = os.path.exists(file_path)
    if not file_exists:
        headers = [
            'test_name', 'run_id', 'test_start', 'query_name', 'query_num', 'status', 'round_trip_ms', 'server_ms', 'queued_ms', 'query_ms', 'network_ms', 'row_count',
            'vi_size', 'agg_par', 'cqel', 'cel',
            'error'
        ]
        with open(file_path, 'wt') as new_details:
            writer = csv.writer(new_details, delimiter = ',')
            writer.writerow(headers)

    test_name = config['test_name']
    run_id = config['stats']['run_id']
    test_start = config['stats']['test_start']

    # Add detail records to the details file
    with open(file_path, 'at') as new_details:
        writer = csv.writer(new_details, delimiter = ',')
        for result in query_results['query_results']:
            line = []
            line.append(test_name)
            line.append(run_id)
            line.append(test_start)
            # Need to add query set identification
            line.append(result['name'])
            line.append(result['query_num'])
            line.append(result['status'])
            if result['status'] == 'success':
                line.append(result['round_trip_ms'])
                line.append(result['server_ms'])
                line.append(result['queued_ns'])
                line.append(result['query_ms'])
                line.append(result['network_ms'])
                if result['row_count'] == 'dropped':
                    line.append(None)
                else:
                    line.append(result['row_count'])
            else: # non successful query
                line.extend([None,None,None,None,None,None])

            line.append(config['target']['vi_size'])
            line.append(config['target']['aggregator_parallelism'])
            line.append(config['target']['concurrent_query_execution_limit'])            
            line.append(config['target']['concurrent_queries_limit'])

            if result['status'] == 'error':
                line.append(result['message'])
            elif result['status'] == 'timeout':
                line.append( 'Query timed out')
            elif result['status'] == 'exhausted':
                line.append( 'Resoruces exhausted')
            writer.writerow(line)

def log_qs_summary(options, config, summary):
    validate_history_dir(options)
    output_dir = options['output_dir']

    # Make sure the history files exists
    file_name = options['qs_summary_name'] 
    file_path = output_dir + '/' + file_name
    file_exists = os.path.exists(file_path)
    if not file_exists:
        headers = [
            'test_name', 'run_id','test_start', 'clean', 'total_ms', 'query_ms', 'queued_ms', 'network_ms',
            'vi_size', 'agg_par', 'cqel', 'cel'
        ]
        with open(file_path, 'wt') as new_summary:
            writer = csv.writer(new_summary, delimiter = ',')
            writer.writerow(headers)

    test_name = config['test_name']
    run_id = config['stats']['run_id']
    test_start = config['stats']['test_start']
  

    # Add detail records to the details file
    with open(file_path, 'at') as new_details:
        writer = csv.writer(new_details, delimiter = ',')
        line = []
        line.append(test_name)
        line.append(run_id)
        line.append(test_start)
        line.append(summary['clean'])
        line.append(summary['total_ms'])
        line.append(summary['query_ms'])
        line.append(summary['queued_ms'])
        line.append(summary['network_ms'])
        line.append(config['target']['vi_size'])
        line.append(config['target']['aggregator_parallelism'])
        line.append(config['target']['concurrent_query_execution_limit'])            
        line.append(config['target']['concurrent_queries_limit'])

        writer.writerow(line)




if __name__ == "__main__":
    options = parse_args()
    config = load_config(options)
    verbose = options['verbose']
    log_output = options['log_output']
    query_results = run_queryset(config['target'], config['queries'])
    obfuscate_apikey(config)
    query_set_summary = summarize_qs_results(config, query_results)
    if verbose:
        display_qs_results(config, query_results)
        display_qs_summary(config, query_set_summary)
    if log_output:
        log_query_results(options, config, query_results)
        log_qs_summary(options, config, query_set_summary)
