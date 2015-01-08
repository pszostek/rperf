#!/usr/bin/env python
# Author: Georgios Bitzes
# georgios.bitzes@cern.ch

from __future__ import print_function
from __future__ import division

import math, subprocess, sys, os, time, argparse, threading, multiprocessing
__version__ = "0.8"

# Write to multiple streams at once
class Tee(object):
    def __init__(self, one, two):
        self.one = one
        self.two = two
    def write(self, message):
        self.one.write(message)
        self.two.write(message)
    def flush(self):
        self.one.flush()
        self.two.flush()

class Spawn(threading.Thread):
    def __init__(self, command, instance, run, filename=None, pinning=None, wait=0):
        super(Spawn, self).__init__()
        
        cmd = command.replace("---INST---", str(instance))
        cmd = cmd.replace("---RUN---", str(run))
        if pinning:
            self.core = pinning[ instance % len(pinning) ]
            cmd = cmd.replace("---CORE---", str(self.core))
            cmd = "taskset -c {0} {1}".format(str(self.core), cmd)

        self.command = cmd
        self.filename = None
        if filename:
            self.filename = "{0}.inst-{1}.run-{2}".format(filename, instance, run)
        self.wait = wait
    def run(self):
        if self.wait > 0:
            sleep(self.wait)
        
        self.start = time.time()
        if self.filename:
            stdout = open(self.filename+".stdout", "w")
            stderr = open(self.filename+".stderr", "w")
            subprocess.call(self.command, shell=True, stdout=stdout, stderr=stderr)
        else:
            subprocess.call(self.command, shell=True, stdout=sys.stdout, stderr=sys.stderr)
        self.end = time.time()
        self.elapsed = self.end - self.start

        if self.filename:
            print("---", file=stdout)
            print("Start time: {0}".format(self.start), file=stdout)
            print("End time: {0}".format(self.end), file=stdout)
            print("Elapsed: {0}".format(self.elapsed), file=stdout)

            stdout.close()
            stderr.close()

def getargs():
    parser = argparse.ArgumentParser(description='A script to launch simultaneous multiple instances of an executable')
    parser.add_argument('--instances', type=int, nargs='+', default=[1], help="The number of simultaneous instances to launch - can be a list")
    parser.add_argument('--runs', type=int, default=1, help="The number of consecutive runs")
    parser.add_argument('--results', type=str, nargs='?', help="The folder in which to store results - if not specified, all output will go to stdout/stderr")
    parser.add_argument('--base', type=float, nargs='?', help="The base run time for 1 instance based on which the scalability factor will be calculated")
    parser.add_argument('--force', dest="force", action='store_true', help="Ignore errors")
    parser.set_defaults(force=False)
    parser.add_argument('--pinning', type=int, nargs='+', help="The order in which the instances will be assigned into each CPU. If there are more instances than there are items in this list, the pinning will wrap")
    parser.add_argument('--command', required=True, type=str, nargs=argparse.REMAINDER, help="The terminal command to be run. Should be the last argument, as the rest of the string will be interpreted to be part of the command to run, not as parameters for this script")
    parser.add_argument('--version', action='version', version='%(prog)s {0}\n'.format(__version__))

    return parser.parse_args()

def verifypinning(pinning, force):
    cores = multiprocessing.cpu_count()
    for i in pinning:
        if i > cores:
            print("Warning: Invalid value {0} in pinning - number of cores is {1} so values in pinning order should be between 0 and {2}".format(i, cores, cores-1))            

            if not force:
                print("Aborting. If sure you want to do this, use --force")
                sys.exit(2)

def mean(l):
    return sum(l)/len(l)

def stddev(l):
    m = mean(l)
    m2 = mean([x**2 for x in l])
    return math.sqrt(m2 - m*m)

def main():
    args = getargs()
    args.command = " ".join(args.command)

    # Create results directory
    if args.results:
        if os.path.exists(args.results):
            print("Results path {0} already exists - will not overwrite existing data".format(args.results))
            sys.exit(1)
        os.makedirs(args.results)

    # Verify pinning order is sane
    if args.pinning:
        verifypinning(args.pinning, args.force)

    # Record the options script was run with
    if args.results:
        with open(os.path.join(args.results, "options"), "w") as f:
            for i in vars(args):
                f.write("{0}: {1}\n".format(i, getattr(args, i)))

    # Where will the reports be written?
    if args.results:
        freport = open(os.path.join(args.results, "report"), "w")
        report = Tee(sys.stdout, freport)
    else:
        report = sys.stdout
        workloadfile = None

    runtimes = {}
    sums = {}
    base = None
    for instances in args.instances:
        print("Instances {0}".format(instances), file=report)
        if args.results:
            workloadfile = os.path.join(args.results, "workload_{0}".format(instances))
        runtimes[instances] = {}
        sums = []
        factors = []
        for run in range(0, args.runs):
            print("\tRun {0}".format(run), file=report)
            runtimes[instances][run] = []
            spawned = []
            for instance in range(0, instances): 
                s = Spawn(args.command, instance, run, workloadfile, args.pinning)
                s.daemon = True
                s.start()
                spawned.append(s)
            for instance in range(0, instances):
                spawned[instance].join()
                runtimes[instances][run].append(spawned[instance].elapsed)
                print("\t\t{0} sec".format(spawned[instance].elapsed), file=report)
            print("\n\t\tMean: {0} sec".format(mean(runtimes[instances][run])), file=report)
            runsum = sum(runtimes[instances][run])
            sums.append(runsum)
            print("\t\tSum: {0} sec".format(runsum), file=report)
            print("\t\tStandard deviation: {0}".format(stddev(runtimes[instances][run])), file=report)
            if base:
                factor = (base*instances*instances)/(runsum)
                factors.append(factor)
                print("\t\tScaling factor: {0}".format(factor), file=report)

        print("\tAverage sum per run: {0}".format(mean(sums)), file=report)
        print("\tStandard deviation of sums: {0}".format(stddev(sums)), file=report)
        if base:
            print("\tAverage scaling factor for {0} instances: {1}".format(instances, mean(factors)), file=report)
        if not base and instances == 1:
            print("\t*** Setting base to {0} *** ".format(mean(sums)), file=report)
            base = mean(sums)
            print("\t Average scaling factor for 1 instances: {0}".format(mean(sums)/base), file=report) 

    if args.results:
        freport.close()

if __name__ == "__main__":
    main()

