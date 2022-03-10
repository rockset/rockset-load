from output import log_qs_summary, log_query_results
from display import display_qs_summary, display_qs_results
from executors import ParallelQSExecutor, SerialQSExecutor

class TestMode():
    def __init__(self,config, options):
        self.config = config
        self.options = options
        self.verbose = options['verbose']
        self.log_output = options['log_output']

    def run_queryset(self, target, query_set):

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

    def obfuscate_apikey(self, config):
        # We obfuscate the apikey once we are done executing any queries to prevent it from being leaked in any reports
        last4 = config['target']['api_key'][-4:]
        config['target']['api_key'] = '******' + last4

    def summarize_qs_results(self, config, results):
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

class QPSTestMode(TestMode):
    def __init__(self,config, options):
       super().__init__(config,options)

class IterationsTestMode(TestMode):
    def __init__(self,config, options):
       super().__init__(config,options)

    def run(self):
        query_results = self.run_queryset(self.config['target'], self.config['queries'])
        self.obfuscate_apikey(self.config)
        query_set_summary = self.summarize_qs_results(self.config, query_results)
        if self.verbose:
            display_qs_results(self.config, query_results)
            display_qs_summary(self.config, query_set_summary)
        if self.log_output:
            log_query_results(self.options, self.config, query_results)
            log_qs_summary(self.options, self.config, query_set_summary)
  
