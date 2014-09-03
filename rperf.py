#!/usr/bin/env python
# author: Pawel Szostek

from __future__ import print_function
import sys
from optparse import OptionParser
from subprocess import Popen, PIPE
try:
    from collections import OrderedDict
except:
    from OrderedDict import OrderedDict
from threading import Thread
from functools import partial
import re
import os
from termcolor import cprint, colored
from time import localtime, strftime
from prettytable import from_csv
from Queue import Queue


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


def get_list_of_hosts(hostfile_path):
    hostfile = open(hostfile_path, 'r')
    hosts = cut_out_comments(hostfile.readlines())
    return hosts


def parse_perf(perf_output):
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
            parts = line.split(" ")
            if line.startswith("<not supported>"):
                stats[parts[2]] = None  # <not supported> name
            elif line.endswith("seconds time elapsed"):
                stats["time"] = float(parts[0])
            else:
                try:
                    value = int(parts[0].replace(',', ''))  # get rid of commas
                    stats[parts[1]] = value
                except:
                    pass
    return stats


def gather_results(hosts, user, command, verbose, print_stdout, print_stderr):
    results = OrderedDict()
    host_counter = 1
    write_queue = Queue()

    def printer(queue):
        summary_count = 0
        while True:
            message = queue.get()
            if message is None:  # job is done
                break
            else:
                summary_count += 1
                hostsno = str(len(hosts))
                print(
                    "%s %s" %
                    (colored(
                        "[{host}/{hosts}]".format(
                            host=summary_count,
                            hosts=hostsno),
                        "magenta",
                        attrs=["bold"]),
                        message), end="")
            queue.task_done()

    def gather_single(host, user, results, queue):
        def printq(string):
            queue.put(Message(string))

        def prints(string):
            queue.put(Summary(string))

        if user is not None:
            ssh_serv = "%s@%s" % (user, host)
        else:
            ssh_serv = host
        ssh_cmd = ' '.join(["ssh", ssh_serv, cmd])
        #ssh_cmd = cmd
        if verbose:
            printq("Running command %s" % ssh_cmd)

        ssh_serv = Popen(ssh_cmd,
                         shell=True,
                         stdout=PIPE,
                         stdin=PIPE,
                         stderr=PIPE)

        stdout, stderr = ssh_serv.communicate()
        time_ = strftime("%H:%M:%S", localtime())

        output = ""
        if ssh_serv.returncode != 0:
            output += "{time} {success} {host}\n".format(
                hosts=str(len(hosts)),
                time=time_,
                success=colored("[FAILED]", "red"),
                host=host)
        else:
            output += "{time} {success} {host}\n".format(
                hosts=str(len(hosts)),
                time=time_,
                success=colored("[SUCCESS]", "green"),
                host=host
            )

        if verbose or print_stdout:
            if not stdout:
                stdout = ""
            output += "{stdouts}\n{stdout}".format(stdouts=colored("stdout", "green",attrs=['bold']),
                                                   stdout=stdout)

        if verbose or print_stderr:
            if not stderr:
                stderr = ""
            output += "{stderrs}\n{stderr}".format(stderrs=colored("stderr", "green",attrs=['bold']),
                                                   stderr=stderr)
        queue.put(output)
        result = parse_perf(stderr)
        results[host] = result

    workers = [Thread(target=gather_single, args=(host, user, results, write_queue))
               for host in hosts]

    printer_thread = Thread(target=printer, args=(write_queue,))
    printer_thread.start()

    for thread in workers:
        thread.start()

    for thread in workers:
        thread.join()

    write_queue.put(None)
    printer_thread.join()

    return results


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


def output_results(results, output_file, hosts_vertically):
    if hosts_vertically:  # event horizontally
        first_host = results.keys()[0]
        event_names = results[first_host].keys()
        events_str = ',' + ','.join(event_names) + '\n'
        output_file.write(events_str)
        for hostname, values_on_host in results.iteritems():
            row = hostname + ',' + \
                ','.join([str(value) for value in values_on_host.values()])
            output_file.write(row + '\n')

    else:  # hosts horizontally, events vertically
        hosts_str = ',' + ','.join(results.keys()) + '\n'
        output_file.write(hosts_str)
        host_names = results.keys()
        results = flip_dictionary(results)
        for event_name, values_on_hosts in results.iteritems():
            row = event_name + ',' + \
                ','.join([str(value) for value in values_on_hosts.values()])
            output_file.write(row + '\n')


def make_remote_command(arguments, precmd):
    perf_cmd = arguments[:]
    perf_cmd = "perf " + ' '.join(perf_cmd)

    if precmd:
        cmd = options.precmd + " && " + perf_cmd
    else:
        cmd = perf_cmd
    cmd = "'%s'" % cmd
    return cmd

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
        '--dump',
        dest="dump",
        help="Dump results on the stdout",
        action="store_true")
    parser.add_option(
        "--hosts",
        dest="hosts",
        help="Path to the host file",
        metavar="HOSTFILE")
    parser.add_option(
        '-l',
        "--user",
        dest="user",
        help="Remote user",
        metavar="REMOTEUSER")
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
        '--pre',
        dest="precmd",
        help="Command to be executed before perf execution",
        metavar="PRECMD")

    (options, arguments) = parser.parse_args()
    verbose = options.verbose

    if options.hosts is None:
        print("\nPath to the host file was not given.\n")
        parser.print_help()
        sys.exit(1)
    elif not os.path.isfile(options.hosts):
        print("Given path does not point to a file.\n")
        sys.exit(2)

    if options.output and os.path.isfile(options.output) and verbose:
        print("Given output file exists. It will be overwritten.")
    if not options.output and verbose:
        print(
            "No output file given. The results will be printed to the standard output.")

    hosts = get_list_of_hosts(options.hosts)
    cmd = make_remote_command(arguments=arguments,
                              precmd=options.precmd)

    if options.output is not None:
        output_path = options.output
    else:
        output_path = "rperf.csv"

    if verbose:
        print("Output will be written to %s" % output_path)

    if verbose:
        print("Running through hosts file:")
        print(hosts)

    perf_stats = gather_results(hosts=hosts,
                                user=options.user,
                                command=cmd,
                                verbose=verbose,
                                print_stdout=options.inline_stdout,
                                print_stderr=options.inline_stderr)

    with open(output_path, 'w+r') as output_file:
        output_results(results=perf_stats,
                       output_file=output_file,
                       hosts_vertically=options.hosts_vertically)

        if options.dump:
            cprint("\nResults:", "white", attrs=["bold"])
            output_file.seek(0)
            pretty_table = from_csv(output_file)
            print(pretty_table)

    if verbose:
        print("Results have been written to %s" % output_path)
