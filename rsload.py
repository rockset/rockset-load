import os, yaml, requests, uuid
import argparse
from testModes import QPSTestMode, IterationsTestMode
from dotenv import load_dotenv
from datetime import datetime
from testModes import IterationsTestMode

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


if __name__ == "__main__":
    options = parse_args()
    config = load_config(options)
    iterations = 1
    if 'iterations' in config:
        iterations = config['iterations']

    if iterations < 1:
        test = QPSTestMode(config, options) 
    else:
        test = IterationsTestMode(config, options) 

    test.run()