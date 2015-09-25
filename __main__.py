#!/usr/bin/env python
"""
Main file of runstuff
author: Pawel Szostek
"""

from __future__ import print_function
from helper import (flip_dictionary,
                    cut_out_comments,
                    intigerize,
                    median,
                    mean,
                    stddev,
                    make_hash)
from pandas import MultiIndex, DataFrame
from optparse import OptionParser
from subprocess import Popen, PIPE
from itertools import chain, repeat
from copy import copy
try:
    from collections import OrderedDict
except ImportError:
    from OrderedDict import OrderedDict
from threading import Thread, Event
import os
import sys
from termcolor import cprint, colored
from time import localtime, strftime
from prettytable import from_csv
from Queue import Queue
from collections import deque, defaultdict
from StringIO import StringIO
from collections import namedtuple
import json
import csv

Job = namedtuple('Job', ['host', 'id', 'command', 'conf'])
Stats = namedtuple('Stats', ['min', 'max', 'average', 'median', 'std_dev'])


def get_conf(conf_filename):
    """Reads and sanitizes configuration from a json file"""
    def sanitize_single_run(run):
        run_cpy = copy(run)
        if 'id' not in run_cpy:
            # run_cpy['id'] = run_cpy['binary']
            run_cpy['id'] = None
        if 'host' not in run_cpy:
            run_cpy['host'] = 'localhost'
        if 'env' in run_cpy:
            assert isinstance(run_cpy['env'], dict)
        else:
            run_cpy['env'] = None
        if 'user' not in run_cpy:
            run_cpy['user'] = None
        if 'precmd' not in run_cpy:
            run_cpy['precmd'] = None
        assert 'events' in run_cpy or 'pfm-events' in run_cpy
        if 'events' not in run_cpy:
            run_cpy['events'] = None
        else:
            if isinstance(run_cpy['events'], str):
                run_cpy['events'] = [run_cpy['events']]
        if 'pfm-events' not in run_cpy:
            run_cpy['pfm-events'] = None
        return run_cpy
    with open(conf_filename, 'r') as conf_file:
        try:
            lines = conf_file.readlines()
            content = ''.join(cut_out_comments(lines))
            conf_dict = json.loads(content)
        except:
            conf_file.seek(0)
            conf_dict = list(csv.DictReader(conf_file))

    sanitized = []
    if isinstance(conf_dict, list):
        for item in conf_dict:
            sanitized.append(sanitize_single_run(item))
        return sanitized
    else:
        sanitized = sanitize_single_run(conf_dict)
        return list(sanitized)


def parse_perf(perf_output, include_time):
    lines = [line.strip() for line in perf_output.split("\n")]
    stats_started = False
    stats = OrderedDict()
    for line in lines:
        if line.startswith("Performance counter stats"):
            stats_started = True
            continue
        elif "seconds time elapsed" in lines:
            break
        elif stats_started:
            if not line:
                continue
            parts = line.split()
            if line.startswith("<not supported>"):
                stats[parts[2]] = None  # <not supported> name
            elif line.endswith("seconds time elapsed"):
                if include_time:
                    stats["time"] = float(parts[0])
            else:
                try:
                    value = int(parts[0].replace("'", '').replace(',', ''))
                    # get rid of commas
                    stats[parts[1]] = value
                except IndexError:  # OK, there is nothing in this line
                    pass
    return stats


def get_stats(results):
    """ Produces statistics for various metrics from many runs
        INPUT:
            results is a dictionary: {id : [{}, {}, {}]}
        OUTPUT:
            DataFrame with statistics on different parameters
    """

    ret = OrderedDict()
    for id_, res_list in results.items():
        average_res = OrderedDict()
        metrics = res_list[0].keys()
        for metric in metrics:
            try:
                metric_results = [float(result[metric]) for result in res_list]
                stats = Stats(intigerize(min(metric_results)),
                              intigerize(max(metric_results)),
                              intigerize(mean(metric_results)),
                              intigerize(median(metric_results)),
                              intigerize(stddev(metric_results)))
                average_res[metric] = stats
            except ValueError:
                # we fall back here is every run was executed only once
                average_res[metric] = res_list[0][metric]
        ret[id_] = average_res
    return ret

class PrinterThread(Thread):
    def __init__(self, stdout_queue, total_jobs):
        super(PrinterThread, self).__init__()
        self._stop = Event()
        self.stdout_queue = stdout_queue
        self.total_jobs = total_jobs

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def run(self):
        summary_count = 0
        while not self._stop.isSet():
            message = self.stdout_queue.get()
            if message is None:  # job is done
                break
            else:
                summary_count += 1
                print(
                    "%s %s" %
                    (colored(
                        "[{job}/{total_jobs}]".format(
                            job=summary_count,
                            total_jobs=self.total_jobs),
                        "magenta",
                        attrs=["bold"]),
                        message))
            self.stdout_queue.task_done()
        #print("quit print_thread")

def gather_single(job, results, stdout_queue, done_hosts_queue, include_time,
                  verbose, print_stdout, print_stderr, grab_output=False):
    """Calls a single command and returns things to be printed via a queue

    job: an instance of Job
    results: a dictionary to write results to
    stdout_queue: a Queue object to print stuff without races
    done_hosts_queue: a Queue to signalize vacant hosts

    This function is a thread target function.
    """
    if verbose:
        stdout_queue.put("Running command at %s: %s" %
                                        (colored(job.host, attrs=["bold"]),
                                        colored(job.command, attrs=["bold"])))
    command_pipe = Popen(job.command,
                         shell=True,
                         stdout=PIPE,
                         stdin=PIPE,
                         stderr=PIPE)

    stdout, stderr = command_pipe.communicate()
    time_ = strftime("%H:%M:%S", localtime())

    output = ""
    if command_pipe.returncode != 0:
        output += "{time} {failure} {host}".format(
            time=time_,
            failure=colored("[FAILED]", "red"),
            host=job.host)
    else:
        output += "{time} {success} {id}@{host}".format(
            time=time_,
            success=colored("[SUCCESS]", "green"),
            id=job.conf["id"],
            host=job.host
        )
        result = parse_perf(stderr, include_time=include_time)

        if grab_output:
            result["output"] = stdout.strip()

        if job.id in results:
            results[job.id].append(result)
        else:
            results[job.id] = [result]
    done_hosts_queue.put(job.host)

    stream_out = "{stream_name}\n{content}"
    if verbose or print_stdout:
        if not stdout:
            stdout = ""
        output += stream_out.format(
                    stream_name=colored("stdout", "green", attrs=['bold']),
                                                            content=stdout)

    if verbose or print_stderr:
        if not stderr:
            stderr = ""
        output += stream_out.format(
                    stream_name=colored("stderr", "red", attrs=['bold']),
                                                          content=stderr)
    stdout_queue.put(output)

def make_list_of_runs(conf, times, randomize):
    """Makes a list of runs with randomization and repetitions

    conf: configuration file expressed as a list of dictionaries
    times: an integer specifying how many times each command has to be run
    randomize: a boolean value saying whether the runs should be mixed or launched as in config

    Return a list of run dictionaries, possibly randomized
    """
    runs = []
    for conf_run in conf:
        for _ in xrange(times):
            runs.append(conf_run)

    if randomize:
        import random
        random.shuffle(runs)
    return runs

def make_list_of_jobs(conf, times, randomize, work_dir):
    """Generates list of all jobs to be run

    conf: configuration file expressed as a list of dictionaries
    times: an integer specifying how many times each command has to be run
    randomize: a boolean value saying whether the runs should be mixed or launched as in config

    Returns a list of Job instances
    """
    runs = make_list_of_runs(conf, times, randomize)
    jobs = []
    for run in runs:
        command = make_command_string(events=run["events"],
                                      pfm_events=run["pfm-events"],
                                      precmd=run["precmd"],
                                      env=run["env"],
                                      command=run["command"],
                                      host=run["host"],
                                      user=run["user"],
                                      work_dir=work_dir)
        id_ = make_hash(run)
        jobs.append(Job(run["host"], id_, command, run))
    return jobs

def gather_results(jobs, verbose, include_time, grab_output, print_stdout, print_stderr, work_dir):
    """Calls the commands for each host in parallel and returns stats

       jobs is a list of Job instances
       host_commands_dict = {host:[(id,command),(id,command)]}
    """
    results = OrderedDict()
    stdout_queue = Queue()
    done_hosts_queue = Queue()

    # queue of watinig jobs. keys are hostnames, values are waiting threads
    host_jobs = defaultdict(deque)
    jobs_done = 0

    # create all the threads, but don't start them yet
    for job in jobs:
        worker = Thread(
            target=gather_single,
            args=(
                job,
                results,
                stdout_queue,
                done_hosts_queue,
                include_time,
                verbose,
                print_stdout,
                print_stderr,
                grab_output))

        host_jobs[job.host].append(worker)

    printer_thread = PrinterThread(stdout_queue, len(jobs))
    printer_thread.start()

    try:
        # bootstrap one job per host
        for jobs_for_host in host_jobs.values():
            jobs_for_host[0].start()

        while len(jobs) != jobs_done:
            host_done = done_hosts_queue.get()
            host_jobs[host_done][0].join()
            jobs_done += 1
            host_jobs[host_done].popleft()  # forget about terminated thread
            if len(host_jobs[host_done]) != 0:
                host_jobs[host_done][0].start()
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
        for workers in host_jobs.values():
            for worker in workers:
                try:
                    worker.join()
                except RuntimeError: # trying to join unstarted Thread
                    pass
        printer_thread.stop()
        printer_thread.join()
        print(colored("Execution interrupted. %d jobs were finished."
                % jobs_done, "red", attrs=["bold"]))
        sys.exit(1)

    stdout_queue.put(None)
    printer_thread.join()

    return get_stats(results)

def output_results(results, output_buffer, hosts_vertically):
    """Prints nicely all the results to the console"""
    if not results:
        return
    if hosts_vertically:  # event horizontally
        first_id = results.keys()[0]
        event_names = results[first_id].keys()
        stat_names = results[first_id][event_names[0]]._fields
        print(event_names)
        if isinstance(results[first_id][event_names[0]], Stats):
            stats_length = len(results[first_id][event_names[0]])
            events_str = ',' + ','.join(chain(*zip(*repeat(event_names,stats_length)))) + '\n'
            # e.g. L1-dcache-load-misses time
            stat_str = ',' + ','.join(stat_names*len(event_names)) + '\n'
            # e.g. mean min max stddev median
            output_buffer.write(events_str)
            output_buffer.write(stat_str)
            for id_, stats_on_host in results.iteritems():
                for event_name, stats in stats_on_host.iteritems():
                    # stats is instance of Stats
                    row = str(id_) + ',' + ','.join(list(map(str, stats)))
                    output_buffer.write(row)
                output_buffer.write("\n")
        else:
            events_str = ',' + ','.join(event_names) + '\n'
            output_buffer.write(events_str)
            for id_, values_on_host in results.iteritems():
                if id_ is None:
                    id_ = ""
                row = ','.join((id_, ','.join([str(value) for value in values_on_host.values()])))
                output_buffer.write(row + '\n')

    else:  # hosts horizontally, events vertically
        hosts_str = ',' + ','.join(results.keys()) + '\n'
        output_buffer.write(hosts_str)
        results = flip_dictionary(results)
        for event_name, values_on_hosts in results.iteritems():
            row = event_name + ',' + \
                ','.join([str(value) for value in values_on_hosts.values()])
            output_buffer.write(row + '\n')


def make_command_string(events, pfm_events, precmd, env, command, host, user, work_dir):
    """Generates an exact string to be passed to Popen"""
    binary = command
    command = "perf stat"

    if pfm_events:
        command += " --pfm-events %s " % ','.join(pfm_events)

    if events:
        command += " -e %s " % ','.join(events)

    command += binary
    if isinstance(precmd, basestring):
        command = "{precmd} >/dev/null 2>&1 && {command}".format(
            precmd=precmd,
            command=command)
    elif isinstance(precmd, list):
        precmds = '&&'.join(
            ["{precmd} >/dev/null 2>&1 ".format(precmd=precmd_) for precmd_ in precmd])
        command = "{precmds} && {command}".format(
            precmds=precmds,
            command=command)

    if work_dir != None:
        command = "cd %s && %s" % (work_dir, command)

    if env:
        for key, value in env.items():
            command = "export {key}={value} && {rest}".format(
                key=key,
                value=value,
                rest=command)

    if host != "localhost":
        if user:
            ssh_serv = "%s@%s" % (user, host)
        else:
            ssh_serv = host
        command = "ssh {user_server} '{command}'".format(
            user_server=ssh_serv,
            command=command)
    return command

def dict_to_data_frame(dct):
    first_id = dct.keys()[0]
    event_names = dct[first_id].keys()
    rows = []
    if isinstance(dct[first_id][event_names[0]], Stats):
        stat_names = dct[first_id][event_names[0]]._fields
        stats_length = len(stat_names)
        columns_1st_level = list(chain(*zip(*repeat(event_names,stats_length))))
        columns_2nd_level = stat_names*len(event_names)
        index = MultiIndex.from_tuples(zip(columns_1st_level, columns_2nd_level))
        for id_, stats_for_id in dct.iteritems():
            cur_row = []
            for event_name, stats in stats_for_id.iteritems():
                cur_row.extend(stats)
            rows.append(cur_row)
        df = DataFrame(columns=index,
                       index=dct.keys(),
                       data=rows)

        return df
    else:
        for id_, event_values in dct.iteritems():
            cur_row = event_values.values()
            rows.append(cur_row)
        df = DataFrame(columns=event_names,
                       index=dct.keys(),
                       data=rows)
        return df



def main():
    """Processes parameters, then does some action accordingly"""
    parser = OptionParser()
    parser.add_option(
        '-o',
        "--output",
        dest="output",
        help="Name of the output csv file. Default value is rperf.csv",
        metavar="FILE")
    parser.add_option(
        '-n',
        '--times',
        dest="times",
        metavar="N",
        help="Repeat each measurement N times and take an average of the results",
        default=1)
    parser.add_option(
        '-r',
        '--randomize',
        dest="randomize",
        help="Randomize order of runs",
        action="store_true")
    parser.add_option(
        '--dry',
        dest="dry",
        help="Dry run, without executing the command.",
        action="store_true")
    parser.add_option(
        '--dump-commands',
        dest="dump_commands",
        help="Dump executed commands on stdout.",
        action="store_true")
    parser.add_option(
        '--dump-results',
        dest="dump_results",
        help="Dump results on the stdout",
        action="store_true")
    parser.add_option(
        "--conf",
        dest="conf",
        help="Configuration file path (JSON)",
        metavar="CONF_FILE")
    parser.add_option(
        "--work-dir",
        dest="work_dir",
        help="User DIR as the working directory",
        metavar="DIR")
    parser.add_option(
        '--tool',
        dest="tool",
        help="Use TOOL to run the command",
        metavar="TOOL")
    parser.add_option(
        "--inline-stdout",
        dest="inline_stdout",
        help="Print stdout inlined",
        action="store_true")
    parser.add_option(
        "--inline-stderr",
        dest="inline_stderr",
        help="Print stderr inlined",
        action="store_true")
    parser.add_option(
        '-v',
        "--verbose",
        dest="verbose",
        help="Be verbose",
        action="store_true")
    parser.add_option(
        '--hosts-vertically',
        dest="hosts_vertically",
        help="Print hosts vertically instead of horizontally",
        action="store_true")
    parser.add_option(
        "--grab-output",
        dest="grab_output",
        help="Grab output and put as a column in the result table",
        action="store_true")
    parser.add_option(
        "--no-time",
        "--exclude-time",
        dest="no_time",
        help="Do not include time from perf in the result table",
        action="store_true")

    (options, _) = parser.parse_args()

    if options.conf is None:
        print("\nPath to the configuration file was not given.\n")
        parser.print_help()
        sys.exit(1)

    elif not os.path.isfile(options.conf):
        print("Given path does not point to a file.\n")
        sys.exit(2)

    if options.work_dir and not os.path.exists(options.work_dir):
        print("Given working directory does not exist. Exiting.")
        sys.exit(3)

    if options.output and os.path.isfile(options.output) and options.verbose:
        print("Given output file exists. It will be overwritten.")
    if not options.output and options.verbose:
        print(
            "No output file given. The results will be dumped to rperf.csv.")

    output_path = options.output if options.output is not None else "rperf.csv"

    conf = get_conf(conf_filename=options.conf)
    jobs = make_list_of_jobs(conf=conf,
        times=int(options.times),
        randomize=options.randomize,
        work_dir=options.work_dir)

    if options.dump_commands:
        for job in jobs:
            print("%s: %s" % (colored(job.host, attrs=["bold"]), job.command))

    if options.dry:
        return

     # when saving the results,
                        # the key will be a hash instead of user-given id
    perf_stats = gather_results(jobs=jobs,
                                verbose=options.verbose,
                                include_time=(not options.no_time),
                                grab_output=options.grab_output,
                                print_stdout=options.inline_stdout,
                                print_stderr=options.inline_stderr,
                                )

    host_stats_dict = OrderedDict()
    for id_ in perf_stats.keys():
        user_id = next(job.conf["id"] for job in jobs if job.id == id_)
        if not user_id:
            user_id = next(job.host for job in jobs if job.id == id_)
        host_stats_dict[user_id] = perf_stats[id_]
    perf_stats = host_stats_dict

    df = dict_to_data_frame(perf_stats)

    if options.dump_results:
        cprint("\nResults:", "white", attrs=["bold"])
        if options.hosts_vertically:
            print(df)
        else:
            print(df.T)

    df.to_csv(output_path)

    if options.verbose:
        print("Results have been written to %s" % output_path)

if __name__ == "__main__":
    main()
