#!/usr/bin/env python
# author: Pawel Szostek

from __future__ import print_function
import sys
from optparse import OptionParser
from subprocess import Popen, PIPE
try:
    from collections import OrderedDict
except ImportError:
    from OrderedDict import OrderedDict
from threading import Thread
import os
from termcolor import cprint, colored
from time import localtime, strftime
from prettytable import from_csv
from Queue import Queue
from collections import deque, defaultdict
from StringIO import StringIO
import json


def get_conf(conf_file):
    def parse_single(single_run):
        if 'id' not in single_run:
            single_run['id'] = single_run['binary']
        if 'host' not in single_run:
            single_run['host'] = 'localhost'
        if 'env' in single_run:
            assert isinstance(single_run['env'], dict)
        if 'user' not in single_run:
            single_run['user'] = None
        if 'precmd' not in single_run:
            single_run['precmd'] = None
        assert 'events' in single_run or 'pfm-events' in single_run
        if 'events' not in single_run:
            single_run['events'] = None
        if 'pfm-events' not in single_run:
            single_run['pfm-events'] = None

    with open(conf_file, 'r') as conf:
        lines = conf.readlines()
        content = ''.join(lines)
        conf_json = json.loads(content)
    if isinstance(conf_json, list):
        for item in conf_json:
            parse_single(item)
        return conf_json
    else:
        parse_single(conf_json)
        return list(conf_json)


def cut_out_comments(hostfile_lines):
    output = []
    for line in hostfile_lines:
        line = line.strip()
        if '#' in line:
            parts = line.split('#')
            not_commented_out = parts[0]
            if not_commented_out:
                output.append("%s\n" % not_commented_out)
        else:
            output.append(line)
    return output


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
                print(line)
                try:
                    value = int(parts[0].replace(',', ''))  # get rid of commas
                    stats[parts[1]] = value
                except IndexError:  # OK, there is nothing in this line
                    pass
    return stats

def average_results(results):
    # results is a dictionary: {id : [res1, res2, res3]}
    # turn it into {id : res}
    def average(list_):
        return sum(list_)/float(len(list_))
    ret = {}
    for id_, res_list in results.items():
        average_res = {}
        metrics = res_list[0].keys()
        for metric in metrics:
            try:
                average_res[metric] = average([result[metric] for result in res_list])
            except:
                average_res[metric] = res_list[0][metric]
        ret[id_] = average_res
    return ret

def gather_results(
        conf, verbose, include_time, grab_output, print_stdout, print_stderr, times):
    results = OrderedDict()
    stdout_queue = Queue()
    done_hosts_queue = Queue()
    jobs_number = len(conf)*times

    def printer(stdout_queue, total_jobs):
        summary_count = 0
        while True:
            message = stdout_queue.get()
            if message is None:  # job is done
                break
            else:
                summary_count += 1
                print(
                    "%s %s" %
                    (colored(
                        "[{job}/{total_jobs}]".format(
                            job=summary_count,
                            total_jobs=total_jobs),
                        "magenta",
                        attrs=["bold"]),
                        message), end="")
            stdout_queue.task_done()

    def gather_single(command, id_, host, user, results,
                      stdout_queue, done_hosts_queue, grab_output=False):
        if host != "localhost":
            if user:
                ssh_serv = "%s@%s" % (user, host)
            else:
                ssh_serv = host
            command = "ssh {user_server} '{command}'".format(
                user_server=ssh_serv,
                command=command)

        if verbose:
            print("Running command %s" % command)

        command_pipe = Popen(command,
                             shell=True,
                             stdout=PIPE,
                             stdin=PIPE,
                             stderr=PIPE)

        stdout, stderr = command_pipe.communicate()
        time_ = strftime("%H:%M:%S", localtime())

        output = ""
        if command_pipe.returncode != 0:
            output += "{time} {failure} {host}\n".format(
                time=time_,
                failure=colored("[FAILED]", "red"),
                host=host)
        else:
            output += "{time} {success} {host}\n".format(
                time=time_,
                success=colored("[SUCCESS]", "green"),
                host=host
            )
            result = parse_perf(stderr, include_time=include_time)

            if grab_output:
                result["output"] = stdout.strip()

            if id_ in results:
                results[id_].append(result)
            else:
                results[id_] = [result]
        done_hosts_queue.put(host)

        if verbose or print_stdout:
            if not stdout:
                stdout = ""
            output += "{stdout_name}\n{stdout}".format(stdout_name=colored("stdout", "green", attrs=['bold']),
                                                       stdout=stdout)

        if verbose or print_stderr:
            if not stderr:
                stderr = ""
            output += "{stderr_name}\n{stderr}".format(stderr_name=colored("stderr", "red", attrs=['bold']),
                                                       stderr=stderr)
        stdout_queue.put(output)

    # queue of watinig jobs. keys are hostnames, values are waiting threads
    jobs = defaultdict(deque)
    jobs_total = 0
    jobs_done = 0
    runs = []
    for conf_run_id, conf_run in enumerate(conf):
        for _ in xrange(times):
            runs.append((conf_run_id, conf_run))
    for run_id, run in runs: # create all the threads, but don't start them yet
        command = make_remote_command(events=run["events"],
                                      pfm_events=run["pfm-events"],
                                      precmd=run["precmd"],
                                      env=run["env"],
                                      command=run["command"])
        host = run["host"]
        user = run["user"]
        id_ = run["id"]
        job = Thread(
            target=gather_single,
            args=(
                command,
                id_,
                host,
                user,
                results,
                stdout_queue,
                done_hosts_queue,
                grab_output))
        jobs[run["host"]].append(job)
        jobs_total += 1

    printer_thread = Thread(target=printer, args=(stdout_queue, jobs_number))
    printer_thread.start()

    # bootstrap one job per host
    for thread in [hostjobs[0] for hostjobs in jobs.values()]:
        thread.start()

    while jobs_total != jobs_done:
        host_done = done_hosts_queue.get()
        jobs[host_done][0].join()
        jobs_done += 1
        jobs[host_done].popleft()  # forget about terminated thread
        if len(jobs[host_done]) != 0:
            jobs[host_done][0].start()

    stdout_queue.put(None)
    printer_thread.join()

    return average_results(results)


def flip_dictionary(ddict):
    # ddict is a dictionary of dictionaries
    keys1 = ddict.keys()
    keys2 = ddict[keys1[0]].keys()
    rdict = OrderedDict()
    for key in keys2:
        rdict[key] = OrderedDict()
    for key1, value1 in ddict.iteritems():
        for key2, value2 in value1.iteritems():
            rdict[key2][key1] = value2
    return rdict


def output_results(results, output_buffer, hosts_vertically):
    if not results:
        return

    if hosts_vertically:  # event horizontally
        first_host = results.keys()[0]
        event_names = results[first_host].keys()
        events_str = ',' + ','.join(event_names) + '\n'
        output_buffer.write(events_str)
        for hostname, values_on_host in results.iteritems():
            row = hostname + ',' + \
                ','.join([str(value) for value in values_on_host.values()])
            output_buffer.write(row + '\n')

    else:  # hosts horizontally, events vertically
        hosts_str = ',' + ','.join(results.keys()) + '\n'
        output_buffer.write(hosts_str)
        results = flip_dictionary(results)
        for event_name, values_on_hosts in results.iteritems():
            row = event_name + ',' + \
                ','.join([str(value) for value in values_on_hosts.values()])
            output_buffer.write(row + '\n')


def make_remote_command(events, pfm_events, precmd, env, command):
    binary = command
    command = "perf stat "

    if pfm_events:
        command += " --pfm-events %s " % ','.join(pfm_events)

    if events:
        command += " -e %s " % ','.join(events)

    command += binary
    if precmd:
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

    if env:
        for key, value in env.items():
            command = "export {key}={value} && {rest}".format(
                key=key,
                value=value,
                rest=command)

    return command

if __name__ == "__main__":

    output_csv = False

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
        '--dump',
        dest="dump",
        help="Dump results on the stdout",
        action="store_true")
    # parser.add_option(
    #     "--hosts",
    #     dest="hosts",
    #     help="Path to the host file",
    #     metavar="HOSTFILE")
    # parser.add_option(
    #     '-l',
    #     "--user",
    #     dest="user",
    #     help="Remote user",
        # metavar="REMOTEUSER")
    parser.add_option(
        "--conf",
        dest="conf",
        help="Configuration file path (JSON)",
        metavar="CONF_FILE")
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
    # parser.add_option(
    #     '--pre',
    #     dest="precmd",
    #     help="Command to be executed before perf execution",
    #     metavar="PRECMD")
    parser.add_option(
        "--grab-output",
        dest="grab_output",
        help="Grab output and put as a column in the result table",
        action="store_true")
    parser.add_option(
        "--include-time",
        dest="include_time",
        help="Include time from perf in the result table",
        action="store_true")

    (options, _) = parser.parse_args()
    verbose = options.verbose

    if options.conf is None:
        print("\nPath to the configuration file was not given.\n")
        parser.print_help()
        sys.exit(1)
    elif not os.path.isfile(options.conf):
        print("Given path does not point to a file.\n")
        sys.exit(2)

    if options.output and os.path.isfile(options.output) and verbose:
        print("Given output file exists. It will be overwritten.")
    if not options.output and verbose:
        print(
            "No output file given. The results will be dumped to rperf.csv.")

    if options.output is not None:
        output_path = options.output
    else:
        output_path = "rperf.csv"

    conf = get_conf(conf_file=options.conf)

    perf_stats = gather_results(conf=conf,
                                verbose=verbose,
                                include_time=options.include_time,
                                grab_output=options.grab_output,
                                print_stdout=options.inline_stdout,
                                print_stderr=options.inline_stderr,
                                times=int(options.times))

    output_buffer = StringIO()
    output_results(results=perf_stats,
                   output_buffer=output_buffer,
                   hosts_vertically=options.hosts_vertically)

    try:
        if options.dump:
            cprint("\nResults:", "white", attrs=["bold"])
            output_buffer.seek(0)
            pretty_table = from_csv(output_buffer)
            print(pretty_table)
    except:
        pass

    with open(output_path, 'w+r') as output_file:
        output_file.write(output_buffer.getvalue())

    if verbose:
        print("Results have been written to %s" % output_path)
