import os, csv

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
