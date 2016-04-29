"""Middleman script for interacting with single-user servers via sudo.

This script is run as the user via sudo. It takes input via JSON on stdin,
and executes one of two actions:

- kill: send signal to process via os.kill
- spawn: spawn a single-user server

When spawning, uses `{sys.executable} -m jupyterhub.singleuser`, to ensure
that single-user servers are the only things this script grants permission
to spawn.
"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

import errno
import getpass
import json
import pipes
import os
import sys
from string import Template

from subprocess import Popen
from subprocess import check_output
import subprocess

from tornado import log
from tornado.options import parse_command_line
app_log = log.app_log

from bs4 import BeautifulSoup

def finish(data, fp=sys.stdout):
    """write JSON to stdout"""
    json.dump(data, fp)
    app_log.debug("mediator result: %s", data)
    sys.stdout.flush()

def kill(pid, signal):
    """send a signal to a PID"""
    app_log.debug("Sending signal %i to %i", signal, pid)

    if (signal == 0):
        output = subprocess.check_output(["qstat", "-x", str(pid)])
        soup = BeautifulSoup(output, 'xml')
        state = soup.find('job_state').string
	
        if (state == 'R'):
            alive = True
        else:
            alive = False

    if (signal in [2, 9, 15]):
        p = Popen(["qdel", str(pid)],
            cwd=os.path.expanduser('~'),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        streamdata = p.communicate()[0]
        rc = p.returncode
        if (rc == 0):
            alive = False

    finish({'alive': alive})

def spawn(args, env):
    """spawn a single-user server
    
    Takes args *not including executable* for security reasons.
    Start the single-user server via `python -m jupyterhub.singleuser`,
    and prohibit PYTHONPATH from env for basic protections.
    """

    # Extracting port from given args    
    for line in args:
        if "--port=" in line:
            port = line.split("=")[1]

    serialpbs = Template('''
#PBS -S /bin/bash
##PBS -l nodes=1:ppn=1,walltime=$hours:00:00,pvmem=${mem}gb
#PBS -l walltime=$hours:00:00
#PBS -N jupyter
#PBS -r n
#PBS -o /tmp/notebook_$id.log
#PBS -j oe
#PBS -V

module purge
module load python/3.4-anaconda3
source activate /prodigfs/jupyter/anaconda3/

# setup tunnel for notebook
ssh -N -f -R $port:localhost:$port jupyter-test.ipsl.upmc.fr
# setup tunnel for API
ssh -N -f -L 8081:localhost:8081 jupyter-test.ipsl.upmc.fr

    ''')
    mem = 1
    hours = 6
    id = getpass.getuser()
    serialpbs = serialpbs.substitute(dict(mem = mem, hours=hours,  id=id, port=port, PATH="$PATH"))
    serialpbs+='\n'
    #serialpbs+='cd %s' % "notebooks"
    serialpbs+='\n'
    #cmd+=' > /tmp/jupyter.log 2>&1'

    cmd = [sys.executable, '-m', 'jupyterhub.singleuser'] + args
    for k in ["JPY_API_TOKEN"]:
        cmd.insert(0, "export %s='%s';" % (k, env[k]))

    #cmd_s = ' '.join(pipes.quote(s) for s in cmd)
    cmd_s = ' '.join(cmd)
    serialpbs+=cmd_s
    app_log.info("Spawning %s", cmd_s)
    if 'PYTHONPATH' in env:
        app_log.warn("PYTHONPATH env not allowed for security reasons")
        env.pop('PYTHONPATH')
   
    # use fork to prevent zombie process
    # create pipe to get PID from descendant
    r, w = os.pipe()
    if os.fork(): # parent
        # wait for data on pipe and relay it to stdout
        os.close(w)
        r = os.fdopen(r)
        sys.stdout.write(r.read())
    else:
        os.close(r)
        # don't inherit signals from Hub
        os.setpgrp()
        # detach child FDs, to allow parent process to exit while child waits
        null = os.open(os.devnull, os.O_RDWR)
        for fp in [sys.stdin, sys.stdout, sys.stderr]:
            os.dup2(null, fp.fileno())
        os.close(null)
         
        # launch the single-user server from the subprocess
        # TODO: If we want to see single-user log output,
        # we should send stderr to a file
        p = Popen('qsub', env=env,
            cwd=os.path.expanduser('~'),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )

        out = p.communicate(serialpbs.encode())[0].strip()
        jobid = out.decode('ascii').split('.')[0]

        # pipe finish message to parent
        w = os.fdopen(w, 'w')
        finish({'pid': int(jobid)}, w)
        w.close()
        
        # wait for subprocess, so it doesn't get zombified
        p.wait()

def main():
    """parse JSON from stdin, and take the appropriate action"""
    parse_command_line()
    app_log.debug("Starting mediator for %s", getpass.getuser())
    try:
        kwargs = json.load(sys.stdin)
    except ValueError as e:
        app_log.error("Expected JSON on stdin, got %s" % e)
        sys.exit(1)
    
    action = kwargs.pop('action')
    if action == 'kill':
        kill(**kwargs)
    elif action == 'spawn':
        spawn(**kwargs)
    else:
        raise TypeError("action must be 'spawn' or 'kill'")

if __name__ == '__main__':
    main()
