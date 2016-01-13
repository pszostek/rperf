#!/usr/bin/env python
"""
Main file of runstuff
author: Pawel Szostek
"""

from __future__ import print_function
from copy import deepcopy
from helper import (flip_dictionary,
                    cut_out_comments,
                    integerize,
                    median,
                    mean,
                    stddev)
from pandas import MultiIndex, DataFrame
from argparse import ArgumentParser
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
import parsers
import json
import csv


Stats = namedtuple('Stats', ['min', 'max', 'average', 'median', 'std_dev'])
Results = namedtuple('Results', ['stdout', 'stderr', 'return_code'])


class Scheduler(object):
    def __init__(self, jobs):
        self.jobs = jobs

    def run(self, verbose):
        """Calls the commands for each host in parallel and returns stats

           returns a list of tuples: (job, stdout, stderr, status)
        """
        results = OrderedDict()
        stdout_queue = Queue()
        done_hosts_queue = Queue()

        # queue of watinig jobs. keys are hostnames, values are waiting threads
        host_jobs = defaultdict(deque)
        jobs_done = 0

        # create all the threads, but don't start them yet
        for job in self.jobs:
            worker = Thread(
                target=Scheduler.gather_single,
                args=(
                    job,
                    results,
                    stdout_queue,
                    done_hosts_queue,
                    verbose))

            host_jobs[job.host].append(worker)

        printer_thread = PrinterThread(stdout_queue, len(self.jobs))
        printer_thread.start()

        try:
            # bootstrap one job per host
            for jobs_for_host in host_jobs.values():
                jobs_for_host[0].start()

            while len(self.jobs) != jobs_done:
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

        stdout_queue.put((None, None))
        printer_thread.join()

        return results

    @staticmethod
    def gather_single(job, results, stdout_queue, done_hosts_queue,
                      verbose):
        """Calls a single command and returns things to be printed via a queue
        This function is a thread target function.

        job: an instance of Job
        results: a dictionary to write results to
        stdout_queue: a Queue object to print stuff without races
        done_hosts_queue: a Queue to signalize vacant hosts

        As a side effect puts a tuple (job, stdout, stderr, status) in results
        """
        if verbose:
            if job.host == "localhost":
                host_str = ""
            else:
                host_str = " on %s" % job.host
            if job.user_id:
                id_str = "id"
                id_ = str(job.user_id)
            else:
                id_str = "hash"
                id_ = str(job.hash)
            stdout_queue.put((job.job_hash, "Running command {id_str}={id}{host_str}: {cmd}".format(
                                            id_str=id_str,
                                            id=id_, 
                                            host_str=colored(host_str, attrs=["bold"]),
                                            cmd=colored(job.command, attrs=["bold"]),
                                            )))
        if verbose:
            print("Running %s" % job.get_full_command_string())
        command_pipe = Popen(job.get_full_command_string(),
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
                id=job.user_id,
                host=job.host
            )

        result = Results(stdout, stderr, command_pipe.returncode)
        try:
            results[job.conf_hash].append(result)
        except KeyError:
            results[job.conf_hash] = [result]
        done_hosts_queue.put(job.host)

        stream_out = "{stream_name}\n{content}"
        if verbose:
            if not stdout:
                stdout = ""
            output += stream_out.format(
                        stream_name=colored("stdout", "green", attrs=['bold']),
                                                                content=stdout)

        if verbose:
            if not stderr:
                stderr = ""
            output += stream_out.format(
                        stream_name=colored("stderr", "red", attrs=['bold']),
                                                              content=stderr)
        stdout_queue.put((job.job_hash, output))


class Job(object):
    def __init__(self, user_id, env,
        host, user, precmd, command, perf_events,
        pfm_events, work_dir, tool, conf, seq):
        self.user_id = user_id
        self.env = env
        self.host = host
        self.user = user
        self.precmd = precmd
        self.command = command
        self.perf_events = perf_events
        self.pfm_events = pfm_events
        self.work_dir = work_dir
        self.tool = tool
        self.conf = conf
        self.seq = seq
        self.job_hash = Job.make_hash_(self.__dict__)
        self_dict = deepcopy(self.__dict__)
        del self_dict["seq"]
        del self_dict["job_hash"]
        self.conf_hash = Job.make_hash_(self_dict)

    def get_full_command_string(self):
        """Generates an exact string to be passed to Popen"""
        binary = self.command
        command = ""

        if self.tool == 'perf':
            command += "perf stat "
            if self.pfm_events:
                command += "--pfm-events %s " % ','.join(self.pfm_events)
            if self.perf_events:
                command += "-e %s " % ','.join(self.perf_events)
        elif self.tool == "time":
            command += "/usr/bin/time -f \"%U %S %E\" "

        command += binary
        if isinstance(self.precmd, basestring):
            command = "{precmd} >/dev/null 2>&1 && {command}".format(
                precmd=self.precmd,
                command=command)
        elif isinstance(self.precmd, list):
            precmds_str = '&&'.join(
                ["{precmd} >/dev/null 2>&1 ".format(precmd=precmd_) for precmd_ in self.precmd])
            command = "{precmds} && {command}".format(
                precmds=precmds_str,
                command=command)

        if self.work_dir != None:
            command = "cd %s && %s" % (self.work_dir, command)

        if self.env:
            for key, value in self.env.items():
                command = "export {key}={value} && {rest}".format(
                    key=key,
                    value=value,
                    rest=command)

        if self.host != "localhost":
            if self.user:
                ssh_serv = "%s@%s" % (self.user, self.host)
            else:
                ssh_serv = self.host
            command = "ssh {user_server} '{command}'".format(
                user_server=self.ssh_serv,
                command=self.command)
        return command

    @staticmethod
    def make_hash_(obj):
        """
        http://stackoverflow.com/questions/5884066/hashing-a-python-dictionary

        Makes a hash from a dictionary, list, tuple or set to any level,
        that contains only other hashable types (including any lists, tuples,
        sets and dictionaries).
        """
        if isinstance(obj, (set, tuple, list)):
            return tuple([Job.make_hash_(e) for e in obj])
        elif not isinstance(obj, dict):
            return hash(obj)

        new_dict = deepcopy(obj)
        for key, value in new_dict.items():
            new_dict[key] = Job.make_hash_(value)

        return hash(tuple(frozenset(sorted(new_dict.items()))))


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
        if 'work_dir' not in run_cpy:
            run_cpy['work_dir'] = None
        if 'tool' not in run_cpy:
            run_cpy['tool'] = None 
        if 'user' not in run_cpy:
            run_cpy['user'] = None
        if 'precmd' not in run_cpy:
            run_cpy['precmd'] = None
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
        hash_to_counter = {}
        job_ids_done = set()
        while not self._stop.isSet():
            (hash_, message) = self.stdout_queue.get()
            job_ids_done.add(hash_)
            if message is None:  # job is done
                break
            else:
                print(
                    "%s %s" %
                    (colored(
                        "[{jobs_done}/{total_jobs}]".format(
                            jobs_done=len(job_ids_done),
                            total_jobs=self.total_jobs),
                        "magenta",
                        attrs=["bold"]),
                        message))
            self.stdout_queue.task_done()
        #print("quit print_thread")


def make_list_of_jobs(conf, times, randomize, tool):
    """Generates list of all jobs to be run

    conf: configuration file expressed as a list of dictionaries
    times: an integer specifying how many times each command has to be run
    randomize: a boolean value saying whether the runs should be mixed or launched as in config

    Returns a list of Job instances
    """
    jobs = []
    for conf_entry in conf:
        for seq in xrange(times):
            if not tool:
                tool = conf_entry["tool"]
            job = Job(user_id=conf_entry["id"],
                      env=conf_entry["env"],
                      host=conf_entry["host"],
                      user=conf_entry["user"],
                      precmd=conf_entry["precmd"],
                      command=conf_entry["command"],
                      perf_events=conf_entry["events"],                
                      pfm_events=conf_entry["pfm-events"],
                      work_dir=conf_entry['work_dir'],
                      tool=tool,
                      seq=seq,
                      conf=conf)
            jobs.append(job)
    return jobs


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


def sanitize_args(options):
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
    options.times = int(options.times)


def results_to_data_frame(results, jobs, stats=False, numeric_output=False):
    # results is a dictionary {hash:Results}
    # jobs
    hash_to_job = {job.conf_hash:job for job in jobs}
    ret_dict = defaultdict(list)
    for hash_, partial_results in results.iteritems():
        # partial_results is a list of dictionaries with results
        # for the runs
        tool = hash_to_job[hash_].tool
        if tool == "perf":
            parse_fun = parsers.parse_perf
        elif tool == "time":
            parse_fun = parsers.parse_time
        elif tool is None:
            parse_fun = parsers.parse_notool

        for result in partial_results:
            result_dict = parse_fun(result.stdout, result.stderr, numeric_output=numeric_output)
            user_id = hash_to_job[hash_].user_id
            ret_dict[user_id].append(result_dict)
    if stats:
        stat_funs = [mean, median, stddev]
        stat_names = ["mean", "median", "stddev"]
        metrics = ret_dict[ret_dict.keys()[0]][0].keys()
        data = []
        for hash_, list_of_dicts in ret_dict.iteritems():
            current_row = []
            for metric in metrics:
                for stat_fun in stat_funs:
                    current_row.append(stat_fun([dct[metric] for dct in list_of_dicts]))
            data.append(current_row)
        columns_1st_level = list(chain(*zip(*repeat(metrics,len(stat_funs)))))
        columns_2nd_level = stat_names*len(metrics)
        column_index = MultiIndex.from_tuples(zip(columns_1st_level, columns_2nd_level))
        return DataFrame(index=ret_dict.keys(), data=data, columns=column_index)

    else:
        runs_per_job = len(jobs)/len(hash_to_job)
        index_1st_level = list(chain(*zip(*repeat(ret_dict.keys(),runs_per_job))))
        index_2nd_level = range(runs_per_job)*len(ret_dict)
        index = MultiIndex.from_tuples(zip(index_1st_level, index_2nd_level))
        return DataFrame(index=index, data=list(chain(*ret_dict.values())))

    return DataFrame.from_dict(ret_dict, orient='index')


def main():
    """Processes parameters, then does some action accordingly"""
    parser = ArgumentParser()
    parser.add_argument(
        '-o',
        "--output",
        dest="output",
        help="Name of the output csv file. Default value is rperf.csv",
        metavar="FILE")
    parser.add_argument(
        '-n',
        '--times',
        dest="times",
        metavar="N",
        type=int,
        help="Repeat each measurement N times and take an average of the results",
        default=1)
    parser.add_argument(
        '-r',
        '--randomize',
        dest="randomize",
        help="Randomize order of runs",
        action="store_true")
    parser.add_argument(
        '--dry',
        dest="dry",
        help="Dry run, without executing the command.",
        action="store_true")
    parser.add_argument(
        '--dump-commands',
        dest="dump_commands",
        help="Dump executed commands on stdout.",
        action="store_true")
    parser.add_argument(
        '--dump-results',
        dest="dump_results",
        help="Dump results on the stdout",
        action="store_true")
    parser.add_argument(
        "--conf",
        dest="conf",
        help="Configuration file path (JSON)",
        metavar="CONF_FILE")
    parser.add_argument(
        "--inline-stdout",
        dest="inline_stdout",
        help="Print stdout inlined",
        action="store_true")
    parser.add_argument(
        "--inline-stderr",
        dest="inline_stderr",
        help="Print stderr inlined",
        action="store_true")
    parser.add_argument(
        '-v',
        "--verbose",
        dest="verbose",
        help="Be verbose",
        action="store_true")
    parser.add_argument(
        '--transpose',
        dest="transpose",
        help="PTranspose the result matrix before printing",
        action="store_true")
    parser.add_argument(
        "--grab-output",
        dest="grab_output",
        help="Grab output and put as a column in the result table",
        action="store_true")
    parser.add_argument(
        "--work-dir",
        dest="work_dir",
        metavar="DIR",
        help="Change the working directory to DIR when running the commands")
    parser.add_argument(
        "--numeric-output",
        dest="numeric_output",
        default=False,
        help="Treat output as numerical value and include it in the result",
        action="store_true")
    parser.add_argument(
        "--tool",
        dest="tool",
        default=None,
        help="Tool to be used when executing the command (available are: perf, time)."
             "This option overrides the tool specified in the configuration file")
    parser.add_argument(
        '--stats',
        dest='stats',
        help="Include extended statistics of the measured values",
        action="store_true")

    options= parser.parse_args()
    sanitize_args(options)

    output_path = options.output if options.output is not None else "rperf.csv"

    conf = get_conf(conf_filename=options.conf)
    jobs = make_list_of_jobs(conf=conf,
        times=int(options.times),
        randomize=options.randomize,
        tool=options.tool)
    assert(len(jobs) == len(conf) * options.times)

    if options.dump_commands:
        for job in jobs:
            print("%s: %s" % (colored(job.host, attrs=["bold"]), job.command))

    if options.dry:
        return

     # when saving the results,
                        # the key will be a hash instead of user-given id
    scheduler = Scheduler(jobs)
    results = scheduler.run(verbose=options.verbose)

    df = results_to_data_frame(results=results,
                               jobs=jobs,
                               stats=options.stats,
                               numeric_output=options.numeric_output)


    # host_stats_dict = OrderedDict()
    # for id_ in perf_stats.keys():
    #     user_id = next(job.conf["id"] for job in jobs if job.id == id_)
    #     if not user_id:
    #         user_id = next(job.host for job in jobs if job.id == id_)
    #     host_stats_dict[user_id] = perf_stats[id_]
    # perf_stats = host_stats_dict

    if options.dump_results:
        cprint("\nResults:", "white", attrs=["bold"])
        if options.transpose:
            print(df.T)
        else:
            print(df)

    df.to_csv(output_path)

    if options.verbose:
        print("Results have been written to %s" % output_path)

if __name__ == "__main__":
    main()
