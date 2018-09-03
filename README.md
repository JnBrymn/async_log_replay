# Load Testing Elasticsearch by Replaying Slow Logs

I found a neat idea in Florian Hopfs blog [_Logging Requests to Elasticsearch_](http://blog.florian-hopf.de/2016/03/logging-requests-to-elasticsearch.html)

If you set all of the slowlog thresholds to 0s then you will log all of the traffic coming through elasticsearch in its entirety.
```yaml
index.search.slowlog.threshold.query.debug: 0s
index.search.slowlog.threshold.fetch.debug: 0s
index.indexing.slowlog.threshold.index.debug: 0s
```

So for the rest of this post let's assume that you have set the slow loggers to 0s and recorded 100% of production traffic for an hour for all of your elasticsearch servers.

And I combined this with my newfound knowledge of Python3's asyncio library (thanks largely to Yeray Diaz's posts [_AsyncIO for the Working Python Developer_](https://hackernoon.com/asyncio-for-the-working-python-developer-5c468e6e2e8e) and [_Asyncio Coroutine Patterns: Beyond await_](https://medium.com/python-pandemonium/asyncio-coroutine-patterns-beyond-await-a6121486656f)). The end result is a simple Elasticsearch load tester.

Here's how you run it:

```bash
$ source activate your_python_3.7_environment
$ python load_tester.py --log_file='~/load_testing/machine_1_elasticsearch_slowlog.log' --host='qa-core-es3' --port=9200 --speed_multiplier=2 --run_time_minutes=10
```

Given the specified log file, `load_tester.py` parses the log lines, pulls out the timestamp and the requests (e.g. the URL and the JSON body of the queries). Then each request is sent to the specified host/port to simulate the production load. One neat thing about this setup is that the simulated load does not just contain the same requests, but they are played at the same relative times as the original traffic. So this has the potential to be a very close approximation to real traffic. What's more, you have the option to specify a `speed_multiplier` parameter that controls how fast the logs are played back. Want so see how well your cluster performs under 2x the production traffic? then set `speed_multiplier=2` and you'll know! The final parameter is `run_time_minutes` which, as you would guess, tells how many minutes to run the simulation. If your production log runs out before `run_time_minutes` is up `load_tester.py` starts over at the beginning of the logs.

## Internal Design

The design is composed of 4 classes: 

* `ElasticsearchRequestLogParser` - This takes a slowlog file and parses out the timestamp and the requests(e.g. the URL and the JSON body of the queries) and wraps this in the `Request` object.
* `LoadTester` - This is the central class, it:
  * Consumes requests from a request generator (for now just `ElasticsearchRequestLogParser`)
  * Calculates an appropriate time to sleep before sending out the next request (based on the timestamp int the log, the wallclock time and the specified `speed_multiplier`).
  * Sends the requests asynchronously to the specified host/port/URL.
  * And as the responses come it, the `LoadTester` processes them with a callback to collect information (`ElasticsearchResponseAccumulator` below).
  * After the `run_time_minutes` is completed the remaining outstanding requests are canceled and the results are returned.
* `Request` - This is a simple holder of request information (timestamp, http method, url, and josn body).
* `Response` - This is a simple holder of response information, currently just the status and the JSON response body.
* `ElasticsearchResponseAccumulator` - This accumulates the appropriate information from the responses. Currently this keeps track of the counts of each response status and the `took` time for each elasticsearch query. The `ElasticsearchResponseAccumulator` also provides summary information based upon this accumulated information (shown below).

The design is reasonably generic. For instance, the `LoadTester` object doesn't know it's dealing with Elasticsearch, it just receives a stream of `(timestamp, url, http_verb, json_body)` and it knows how to send these requests off at the appropriate time to some other specified host/port. So in principle you could make some other request generator in the pattern of `ElasticsearchRequestLogParser`


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

The `run_information` tells you some basic information about the run agnostic of Elasticsearch. For instance we can see that in the 1 minute that the scrip was running we sent over 53K requests to elasticsearch averaging just over 895 requests per second. Not bad right!? At the end of the run there were 33 outstanding requests. `seconds_behind` represents how many seconds (wallclock time) ago the last request _should_ have been sent out in order to be delivered on time. 0 indicates that at 895 requests per second, the simulation is keeping up just fine! If I crank up the `speed_multiplier` to some insanely high level then my laptop will average 1267 requests per second.

The `accumulator_information` is the information collected and summarized by `ElasticsearchResponseAccumulator`. In this case we see that all the requests have succeeded 100% of the time. But, keep in mind that asyncio is still single processor. I can spin up 8 copies of this process each one will average about 390 requests per second for a total of about 3,500 requests per second. Wit this load the completion statuses are now a mixture of mostly 200s, a few `503 Service Unavailable`, and a touch of `429 Too Many Requests`.

So long as you can keep up with the load (e.g. the `seconds_behind` should remain near 0) then it makes a lot of sense to spin up several runs like this on one machine. In principle each load simulation would be responsible for replaying the log file from one of the production servers to a clone cluster in Staging.

## Still to do

There are plenty of things we can do to improve the current code.

* Elasticsearch Stuff:
  * In the log file I'm not sure if I'm handling the `source` and `extra_source`. Right now I'm just updating the `source` with the `extra_source` so that any keys that are present in both places will be overwritten by `extra_source`.
  * Right now I'm only dealing with Elasticsearch queries. Even though I set the indexing threshold to 0s I didn't see any index lines show up in the slow log. Wonder why? 🤔
  * The slow log not only contains query and index lines but also fetch lines and lines labeled `QUERY_THEN_FETCH`. Fetching is done internally to retrieve the data associated with the documents matching the queries. I'm not sure how I should simulate these lines or if they should even be simulated at all.
  * Lots of our traffic is batched queries and updates. I think that the slow log splits these out into individual requests. This makes sense when you're trying to diagnose why certain queries are slow, but when simulating traffic, it could cause unrealistically high network usage.
  * *OR* we can circumvent all of these questions by just logging all of the HTTP traffic going into Elasticsearch in some sort of proxy layer.
* General Stuff:
  * Since this is my first stab at `asyncio` I'm sure there are things I could improve with my usage of `asyncio`.
  * Currently I'm treating requests as just a timestamp, URL, http method, and JSON body. I need to generalize this to take any type of body, content-type, and maybe also take header information.
  * Similarly I'm treating responses as just a return status and a JSON. I need to generalize this.
  * The output information at the end of each run is basically proof-of-concept. Both `run_information` and `accumulator_information` need more information about how the run went both generally and specific to Elasticsearch.
  * The load tester needs to print out periodic status messages.
* Nice to Have in the Future:
  * Connecting the thing to a neato aiohttp-based web app.
  * Updating charts and graphs.
  * Coordination across machines for massively distributed load testing.




** don't forget to discuss with Florian, Zeus, and Andrew
** tweet
** post code
** tell Doug


Question to Florian: how do account for query vs. fetch - do I have to simulate fetch? what about QUERY_AND_FETCH that I sawa sometimes? Anything else?
