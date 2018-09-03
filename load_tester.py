"""
TODO
* collect statistics on responses and present back in the final result
"""
# must be run with python3.7
# $ source activate ipykernel_py37
import argparse
import asyncio
from collections import Counter
import datetime
import dateutil.parser
import json
import re
import time

import aiohttp


class LoadTester:
    def __init__(self, loop, request_generator_factory, response_accumulator, host, port, speed_multiplier=1):
        """
        :param loop: the event loop to run on
        :param request_generator_factory: takes no args and returns a request_generator
        :param response_accumulator: takes a dictionary of information about the response and collects useful information
        :param host:
        :param port:
        :param speed_multiplier:
        """
        self.loop = loop
        self.request_generator_factory = request_generator_factory
        self.response_accumulator = response_accumulator
        self.host = host
        self.port = port
        self.speed_multiplier = speed_multiplier
        self.log_initial_time = datetime.datetime.min  # sentinel value meaning that this hasn't been evaluated
        self.real_initial_time = None
        self.log_delta_time = datetime.timedelta(0)  # sentinel value meaning that this hasn't been evaluated

    async def run(self, run_time_minutes):
        run_time_secs = run_time_minutes*60
        num_sent_requests = 0
        start_time = time.time()
        async with aiohttp.ClientSession(loop=self.loop) as session:
            for request in self._requests():
                delta_secs = time.time() - start_time
                if delta_secs >= run_time_secs:
                    break
                await asyncio.sleep(min(request['sleep_time'], run_time_secs - delta_secs))
                num_sent_requests += 1
                fut = asyncio.ensure_future(
                    self._fetch(session, request['url'], request['body'])
                )
                fut.add_done_callback(self._callback)

            pending_tasks = [
                task for task in asyncio.all_tasks()
                if (
                        not task.done()
                        and not task == asyncio.current_task()
                )
            ]

            num_outstanding_requests = len(pending_tasks)
            for task in pending_tasks:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            seconds_behind = max(-request['sleep_time'], 0)
            return {
                'run_time_minutes': delta_secs/60,
                'num_sent_requests': num_sent_requests,
                'average_requests_per_second': num_sent_requests/delta_secs,
                'num_outstanding_requests': num_outstanding_requests,
                'seconds_behind': seconds_behind,  # the last request we send out should have been sent out this many seconds ago
                'percentage_behind': seconds_behind/delta_secs,  # ideally this should be zero meaning that the simulation is keeping up; max value is 1.0
            }

    def _requests(self):
        while True:
            request_generator = self.request_generator_factory()
            if self.real_initial_time is None:
                self.real_initial_time = datetime.datetime.now()
            elif self.log_delta_time == datetime.timedelta(0):
                # then the log has ended for the 1st time and we need to determine the length of the log using the last
                # request datetime
                self.log_delta_time = request.timestamp - self.log_initial_time

            # every time we loop through the log we move log_initial_time backward the delta time of the log
            self.log_initial_time = self.log_initial_time - self.log_delta_time

            for request in request_generator:
                yield {
                    'url': 'http://{host}:{port}/{url_path}'.format(host=self.host, port=self.port, url_path=request.url_path),
                    'body': request.json_body,
                    'sleep_time': self._get_sleep_time(request.timestamp),
                }

    def _get_sleep_time(self, log_time):
        if self.log_initial_time == datetime.datetime.min:
            self.log_initial_time = log_time
        real_time = datetime.datetime.now()

        # Get real time till next event considering
        log_time_passed_secs = (log_time - self.log_initial_time).total_seconds()
        scaled_time_passed = (real_time - self.real_initial_time).total_seconds() * self.speed_multiplier
        scaled_time_till_next_event = log_time_passed_secs - scaled_time_passed
        real_time_till_next_event = scaled_time_till_next_event / self.speed_multiplier
        return real_time_till_next_event  # a.k.a. sleep_time

    @staticmethod
    async def _fetch(session, url, body):
        async with session.post(url, json=body) as response:
            return Response(
                status=response.status,
                json=await response.json(),
            )

    def _callback(self, fut):
        try:
            self.response_accumulator.process_response_information(fut.result())
        except asyncio.CancelledError:
            pass


class Request:
    """A response object.

    For now, this is basically a placeholder as the API isn't stable. This is consumed by
    ElasticsearchResponseAccumulator.process_response_information
    """
    def __init__(self, timestamp, method, url_path, json_body):
        self.timestamp = timestamp
        self.method = method
        self.url_path = url_path
        self.json_body = json_body


class Response:
    """A response object.

    For now, this is basically a placeholder as the API isn't stable. This is consumed by
    ElasticsearchResponseAccumulator.process_response_information
    """
    def __init__(self, status, json):
        self.status = status
        self.json = json


class ElasticsearchRequestLogParser:
    """Parses request log file and for each request returns request datetime, method, url_path, and json_body of the request."""

    def __init__(self, log_file):
        self.log_file = log_file

    def __iter__(self):
        regex = re.compile(
            r'^\[(?P<timestamp>.*?)\]\[.*?\]\[(?P<request_type>.*?)\]\s*\[(?P<index>.*?)\].*source\[(?P<source>.*)\],\s*extra_source\[(?P<extra_source>.*)\]')

        for line in self.lines_of_log_file():
            match = regex.match(line)

            if not match:
                raise Exception(line)

            match_dict = match.groupdict()

            request_type = match_dict['request_type'].split('.')[-1]
            if request_type != 'query':
                continue

            yield Request(
                timestamp=dateutil.parser.parse(match_dict['timestamp'].split(',')[0]),
                method='POST',
                url_path='/{index}/_search'.format(index=match_dict['index']),
                json_body=self._get_body(match_dict),
            )

    def lines_of_log_file(self):
        # this function is made for simpler testing
        with open(self.log_file, 'r') as f:
            for line in f:
                yield line

    @staticmethod
    def _get_body(match_dict):
        json_body = {}
        if match_dict['source']:
            json_body = json.loads(match_dict['source'])
        if match_dict['extra_source']:
            json_body.update(json.loads(match_dict['extra_source']))
        return json_body


class ElasticsearchResponseAccumulator:
    """
        Takes response dictionaries and accumulates important information.
        The input to ElasticsearchResponseAccumulator.process_response_information comes from the output of
        LoadTester._fetch, but the API here isn't stable yet
    """
    def __init__(self):
        self.total_took_time = 0
        self.completion_status_counts = Counter()

    def process_response_information(self, response):
        self.completion_status_counts.update([response.status])
        if response.status == 200:
            self.total_took_time += response.json['took']

    def get_summary(self):
        return {
            'completion_status_counts': dict(self.completion_status_counts),
            'average_time_per_successful_request': self.total_took_time / self.completion_status_counts[200],
        }


async def main(loop, log_file, host, port, speed_multiplier, run_time_minutes):
    def  request_generator_factory():
        return ElasticsearchRequestLogParser(log_file=log_file)

    response_accumulator = ElasticsearchResponseAccumulator()

    load_tester = LoadTester(
        loop=loop,
        request_generator_factory=request_generator_factory,
        response_accumulator=response_accumulator,
        host=host,
        port=port,
        speed_multiplier=speed_multiplier,
    )
    run_information = await load_tester.run(run_time_minutes)

    return {
        'run_information': run_information,
        'accumulator_information': response_accumulator.get_summary(),
    }


if __name__ == '__main__':
    # run with python load_tester_old.py --log_file='/Users/johnb/Desktop/elasticsearch_slowlog.log' --host='qa-core-es3' --port=9200 --speed_multiplier=100
    parser = argparse.ArgumentParser(description='Load test Elasticsearch by replaying the slowlog file.')
    parser.add_argument(
        '--log_file',
        help='the elasticsearch slowlog file',
    )
    parser.add_argument(
        '--host',
        help='the host to load test',
    )
    parser.add_argument(
        '--port',
        help='the port',
    )
    parser.add_argument(
        '--speed_multiplier',
        type=float,
        help='the speed_multiplier - 1 is realtime, 0.5 is half time',
    )
    parser.add_argument(
        '--run_time_minutes',
        type=float,
        help='amount of wallclock time to keep the load test running',
    )

    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    log_file = '/Users/johnb/Desktop/elasticsearch_slowlog.log'
    host = 'qa-core-es3'
    port = 9200
    speed_multiplier = 100

    run_result = loop.run_until_complete(
        main(
            loop=loop,
            log_file=args.log_file,
            host=args.host,
            port=args.port,
            speed_multiplier=args.speed_multiplier,
            run_time_minutes=args.run_time_minutes,
        )
    )
    loop.close()

    print(json.dumps(run_result, indent=4))
