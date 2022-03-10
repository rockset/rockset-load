from columnar import columnar
from click import style

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

