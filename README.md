## What is *runstuff*?
runstuff is a neat script for running an application on multiple hosts for benchmarking purposes. It takes a configuration file with jobs description as an input and runs them successively on local or remote hosts.
runstuff can parse output of *perf* and output statistics for various events.

## How a configuration file looks like.
The configuration file might be either in CSV or JSON format. An example file might look like this:
<pre>
id,host,command,events
1,host1,sleep 10,context-switches
2,host2,sleep 10,context-switches
2,host3,sleep 10,context-switches
</pre>
*id* is a user-defined string which is not interpreted by runstuff. However, there can't be two runs with the same id. *host* contains hostnames of the hosts where a commands has to be run. *command* is the command itself. *events* contains perf events (**not** pfm-events) which should be passed to perf.
Columns understood by runstuff are: *id*, *host*, *env*, *user*, *precmd*, *events* and *pfm-events*.

## Run parameters

| Parameter        | Description           |
| ------------- |:-------------:|
| -c, --conf      | Path to the configuration file (csv or json) |
| -n, --times    | Tells how many times every command should be run. By default every command is run once. If *times* is bigger than 1, runstuff will yield some simple statistics      |
| -r, --randomize | Randomizes runs instead of launching runs in the order in which they appear in the configuration file |
| --dry | Checks the configuration file, but does not run anything |
| --dump-commands | Tells to dump all the commands to stdout |
| --dump-results | Tells runstuff to print the result table to stdout |
| --inline-stdout | Prints commands' stdout as they are executed |
| --inline-stderr | Same as *--inline-stdout*, but for stderr |
| -v, --verbose | Prints some extra debug information |
| --hosts-vertically | Transposes the output matrix |
| --grab-output | Grab output and put as a column in the result table |
| --no-time | Do not include time from perf in the results table |

## Cookbook
0. Check if a configuration file is correct and dump the commands.
<pre>python runstuff --conf run.csv --dry --dump-commands</pre>
1. Run a set of commands and print the output on the screen.
<pre>python runstuff --conf run.csv --dump-results</pre>
2. Run a set of commands, but repeat eachone 10 times to get rid of annoying fluctuations.
<pre>python runstuff --conf run.csv --times 10</pre>
3. Run a set of commands
