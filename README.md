# Rockset Load Testing Framework

rockset-load

This framework allows you to load Rockset with concurrent queries.

## Target
You specify the target Rockset org and region through the combination of the api server and the api key (see below).  The api server is specified in the `target` attribute and does not require you to include `https:\\` as a prefix (although the code will tolerate the existance of the protocol if you include it).

```
test_name: target sample
target:
    api_server:  api.rs2.usw2.rockset.com
```

## API KEY
The Rockset API key must be specified either:
- in the execution environment as an exported variable
- in a `.env` file in the directory where you execute the program

```
ROCKSET_APIKEY=<insert your key>

```

## Configuration File
You may specify a test configuration file using the `-c` or `--config` options on the command line. The default configuration file is `./resources/config.yaml`


## Naming
It is best practice to name all of these elements in the configuration file:
- the overall test in `test_name`
- each query set in `query_set[].name`
- each query in a query set in `query_set[].queries[].name`

Naming everything sensibly is important for understanding the output since some information is reported for individual queries and some is reported as summarized for multilpe queries.

If you only have a single query set as part of the test, it's likely your `test_name` and `query_set[0].name` will be the same or similar.



## Query Sets
A query set (`query_set`) is a list of queries that you want to execute concurrently. You may have multiple query sets - all will (currently) run concurrently. Having mutilpe query sets is unlikely, but supported.


You can specify multilpe queries for each query set, using yaml notation, under the `query_set[].queries` attribute.
Information about the exectuion of each query will be displayed as well as a summary of the time taken to execute all queries in the query set.

Currently there is no support for executing queries in a query set serially, but that is planned.

You can mix `sql` queries and `lambda` queries in a query set, but each query requires one or the other value to be set.

### Sample Config - SQL code

This example shows using SQL. The SQL code is specified using the `sql` attribute in the query specification. By using the form `sql: >+` you can paste the SQL code from the code editor into the configuration yaml. You must, however, indent the code (select it all and use tab to move it right) properly in the yaml configuration file.


```
test_name: SQL Sample
target:
    api_server:  api.rs2.usw2.rockset.com
query_set:
    - name: sample 1
      queries:
        - name: Count 1
          sql: >+
            SELECT
                *
            FROM 
                _events
    - name: sample 2
      queries:
        - name: Count 2
          sql: >+
            SELECT
                *
            FROM 
                _events

```


### Sample Config - Query Lambda

This example demonstrates using a query lambda as the target. Notice that the URL for the lambda (without the api server) is provided with the `lambda` attribute within a query definition. You should specify the entire URL path (event though parts of it are currently the same for all query lambdas). It doesn't matter if you include or exclude the first backslash in the path.

If using parameters, they must be specified correctly in yaml, and, as such, cannot be directly copied from the Rockset console example.

Please note that 'drop_results' is currently not supported when using query lambdas, and it will be ignored.

```
test_name: QL Sample
target:
    api_server:  api.rs2.usw2.rockset.com
query_set:
    - name: samples
      queries:
        - name: Count 1
          lambda: v1/orgs/self/ws/test/lambdas/order_asset_lookup/tags/latest
          parameters:
            - name: order_id
              type: int
              value: 0
 ```

## Query Options

- parameters

Parameters may be specified for both `sql` and `lambda` queries. 
If using parameters, they must be specified correctly in yaml, and, as such, cannot be directly copied from the Rockset console example.

```
test_name: parameter sample
target:
    api_server:  api.rs2.usw2.rockset.com
query_set:
    - name: samples
      queries:
        - name: Count 1
          lambda: v1/orgs/self/ws/test/lambdas/order_asset_lookup/tags/latest
          parameters:
            - name: order_id
              type: int
              value: 0

```

- pagination

You can enable pagination and configure the number of docs in the initial response as part of simulating the performance of a query that would use pagination in the wild. This also allows you to limit the number of documents returned with any query

Inside the configuration of each query, you can enable pagination as follows:

```
test_name: pagination sample
target:
    api_server:  api.rs2.usw2.rockset.com
query_set:
    - name: samples
      queries:
        - name: test 1
          sql: >+
            SELECT
                *
            FROM 
                _events
          paginate: True
          initial_paginate_response_doc_count: 1000

```

- drop results

You can configure a specific query to drop its results and avoid sending the result set back across the network to the client.  This feature is currently not supported when using `lambda` queries.

```
test_name: drop results sample
target:
    api_server:  api.rs2.usw2.rockset.com
query_set:
    - name: samples
      queries:
        - name: Count 1
          sql: >+
            SELECT
                *
            FROM 
                _events
          drop_results: True
```

## Overrides
You can specify some settings that will override settings in all queries in the test

- Drop results

You can cause all queries to drop their results and avoid the time of returning the result set to the client over the network.  This function is currently unsupported for Query Lambdas.

```
target:
    api_server:  api.rs2.usw2.rockset.com
    overrides:
		drop_results
```

```

## Limitations
- rockset-load is currently only designed to run on a single machine. It does create separate processes to run concurrent queries, but you should ensure that your client machine has sufficient resources to invoke the queries.

## Status
- currently only runs through the queries once.  Support for multiple iterations and multiple simulated users is planned
- multiple query sets in a single test has not been tested
- drop_results option is not supported for query lambdas
- Pagination features have not yet been tested
- Parameters have not been tested for SQL queries