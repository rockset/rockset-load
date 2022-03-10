import time, requests
from urllib.parse import quote_from_bytes
from multiprocessing.pool import Pool

class QuerySetExecutor():
    def __init__(self,target, query_set):
        self.target = target
        self.query_set = query_set

    def run_query(self, query_num, user_num, target, query):

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
            result['name'] = 'unnamed'

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
        elif qryResopnse.status_code == 429:
            result['status'] = 'exhausted'
        elif qryResopnse.status_code != 200:
            result['status'] = 'error'
            result['message'] = f'{qryResopnse.reason}. {qryResopnse.text}'
        else:
            result['status'] = 'success'
            result['round_trip_ms'] = round((end - start) * 1000)
            response_data = qryResopnse.json()
            result['server_ms'] = response_data['stats']['elapsed_time_ms']
            result['queued_ns'] = round(response_data['stats']['throttled_time_micros'])
            result['query_ms'] = round(result['server_ms'] - (result['queued_ns'] /1000))
            result['network_ms'] = result['round_trip_ms'] - result['server_ms']
            if drop_results:
                result['row_count'] = 'dropped'
            else:
                result['row_count'] = len(response_data['results'])

        return result

class ParallelQSExecutor(QuerySetExecutor):

    def __init__(self,target, query_set):
       super().__init__(target, query_set)

    def run(self):
        results = {}
        query_count = len(self.query_set['queries'])
        query_results = [None] * query_count
        with Pool(processes=query_count) as pool:
            tasks = []
            for x in range(0, query_count):
                args = []
                args.append(x + 1)   # query_num
                args.append(0)       # user_num
                args.append(self.target)  # target
                args.append(self.query_set['queries'][x])  # query

                task = pool.apply_async(self.run_query, args)
                tasks.append(task)
            
            pool.close()
            pool.join()

            for task in tasks:
                result = task.get()
                offset = result['query_num']
                query_results[offset -1] = result
        
        results['query_results'] = query_results
        return results


class SerialQSExecutor(QuerySetExecutor):

    def __init__(self,target, query_set):
       super().__init__(target, query_set)

    def run(self):
        results = {}
        query_count = len(self.query_set)
        query_results = [None] * query_count
        with Pool(processes=query_count) as pool:
            tasks = []
            for x in range(0, query_count):
                args = []
                args.append(x + 1)   # query_num
                args.append(0)       # user_num
                args.append(self.target)  # target
                args.append(self.query_set[x])  # query

                task = pool.apply(self.run_query, args)
                tasks.append(task)
            
            pool.close()
            pool.join()

            for task in tasks:
                result = task
                offset = result['query_num']
                query_results[offset -1] = result
        
        results['query_results'] = query_results
        return results