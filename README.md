# Elasticsearch log replay load tester.
In order to run the load tester you first need product logs of ALL traffic to elasticsearch. One way to get this is to set the slow log thresholds to 0s for a short period of time.
```yaml
index.search.slowlog.threshold.query.debug: 0s
index.search.slowlog.threshold.fetch.debug: 0s
index.indexing.slowlog.threshold.index.debug: 0s
```

To run the load tester do this:

```bash
$ source activate your_python_3.7_environment
$ python load_tester.py --log_file='~/load_testing/machine_1_elasticsearch_slowlog.log' --host='qa-core-es3' --port=9200 --speed_multiplier=2 --run_time_minutes=10
```

Given the specified log file, `load_tester.py` parses the log lines, pulls out the timestamp and the request. Then each request is sent to the specified host to simulate the production load. One neat thing about this setup is that the simulated load does not just contain the same requests, but they are played at the same relative times as the original traffic. So this has the potential to be a very close approximation to real traffic. What's more, you have the option to specify a `speed_multiplier` parameter that controls how fast the logs are played back. Want so see how well your cluster performs under 2x the production traffic? All you have to do is set `speed_multiplier=2`. The final parameter `run_time_minutes` tells how many minutes to run the simulation. If your production log runs out before `run_time_minutes` is up `load_tester.py` starts over at the beginning of the logs and reruns the log as many times as needed.

## Internal Design

The design is composed of 4 classes: 

* `ElasticsearchRequestLogParser` - This takes a slow log file and parses out the requests(the URL, the JSON body, and the timestamp) and wraps this in the `Request` object below.
* `LoadTester` - This is the central class, it:
  * Consumes requests from a request generator - for now just `ElasticsearchRequestLogParser`
  * Calculates an appropriate time to sleep before sending out the next request based on the timestamp in the log, the wall clock time and the specified `speed_multiplier`.
  * Sends the requests asynchronously to the specified host.
  * And as the responses come back, the `LoadTester` processes them with a callback to collect information - for now `ElasticsearchResponseAccumulator` described below.
  * After the `run_time_minutes` is completed the remaining outstanding requests are canceled and the results are returned.
* `Request` - This is a simple holder of request information: timestamp, http method, url, and JSON body.
* `Response` - This is a simple holder of response information: response status and the JSON response body.
* `ElasticsearchResponseAccumulator` - This accumulates the appropriate information from the responses. Currently this keeps track of the counts of each response status and the `took` time for each elasticsearch query. The `ElasticsearchResponseAccumulator` also provides summary information based upon this accumulated information as shown below.

The design is reasonably generic. For instance, the `LoadTester` object doesn't know it's dealing with Elasticsearch, it just receives a stream of `Request` object and it knows how to send these requests off at the appropriate time to some other specified host/port. So in principle you could make some other request generator in the pattern of `ElasticsearchRequestLogParser`.


## Sample Run and Output

So let's go ahead an run it. 
 
```bash
$ python load_tester.py --log_file='~/load_testing/machine_1_elasticsearch_slowlog.log' --host='qa-core-es3' --port=9200 --speed_multiplier=10 --run_time_minutes=1
```

 At the end of the run `load_tester.py` prints out some summary information that looks like this:
```json
{
    "run_information": {
        "run_time_minutes": 1.0000119845072428,
        "num_sent_requests": 53720,
        "average_requests_per_second": 895.322603333109,
        "num_outstanding_requests": 33,
        "seconds_behind": 0,
        "percentage_behind": 0.0
    },
    "accumulator_information": {
        "completion_status_counts": {
            "200": 53687
        },
        "average_time_per_successful_request": 4.0092573621174585
    }
}
```

The `run_information` tells you some basic information about the run. For instance we can see that in the 1 minute that the script was running we sent over 53K requests to elasticsearch averaging just over 895 requests per second. Not bad right!? At the end of the run there were 33 outstanding requests. `seconds_behind` represents how many seconds (wall clock time) ago the last request _should_ have been sent out in order to be delivered on time. 0 indicates that at 895 requests per second, the simulation is keeping up just fine! If I crank up the `speed_multiplier` to some insanely high level then my laptop will max out at about 1267 requests per second on one process.

The `accumulator_information` is the information collected and summarized by `ElasticsearchResponseAccumulator`. In this case we see that all the requests have succeeded 100% of the time. But, keep in mind that python only takes advantage of a single cpu core. I can spin up 8 copies of this process and each one will average about 390 requests per second for a total of about 3,500 requests per second. With this load the completion statuses are now a mixture of mostly `200`s, a hand full of `503 Service Unavailable`, and a few `429 Too Many Requests`.

So long as your computer can keep up with the load (e.g. the `seconds_behind` should remain near 0) then it makes a lot of sense to spin up several runs like this on one machine. In principle each load simulation would be responsible for replaying the log file from one of the production servers to a clone test cluster.

## Still To Do

There are plenty of things we can do to improve the current code.

* Elasticsearch stuff:
  * In the log file I'm not sure if I'm handling the `source` and `extra_source` correctly. Right now I'm just updating the `source` with the `extra_source` so that any keys that are present in both places will be overwritten by `extra_source`.
  * Right now I'm only dealing with Elasticsearch queries. Even though I set the indexing threshold to 0s I didn't see any index lines show up in the slow log. Wonder why? 🤔
  * The slow log not only contains query and index lines but also fetch lines and lines labeled `QUERY_THEN_FETCH`. Fetching is done internally to retrieve the data associated with the documents matching the queries. I'm not sure how I should simulate these lines or if they should even be simulated at all.
  * Lots of our traffic is batched queries and updates. I think that the slow log splits these out into individual requests. This makes sense when you're trying to diagnose why certain queries are slow, but when simulating traffic, it could cause unrealistically high network usage.
  * *OR* we can circumvent all of these questions by just logging all of the HTTP traffic going into Elasticsearch in some sort of proxy layer.
* General stuff:
  * Since this is my first stab at `asyncio` I'm sure there are things I could improve the code.
  * Currently I'm treating requests as just a timestamp, URL, http method, and JSON body. I need to generalize this to take any type of body, content-type, and maybe also take header information.
  * Similarly I'm treating responses as just a return status and a JSON. I need to generalize this.
  * The load tester needs to print out periodic status messages.
* Nice to have in the future:
  * Connecting the thing to a neat-o aiohttp-based web app that will let you control the runs.
  * Updating charts and graphs in the web app.
  * Coordination across machines for massively distributed load testing.
