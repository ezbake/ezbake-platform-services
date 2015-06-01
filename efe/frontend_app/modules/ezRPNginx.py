#   Copyright (C) 2013-2015 Computer Sciences Corporation
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import os
import signal
import ezRPConfig as gConfig
from shutil import rmtree
from shutil import copyfile
from shutil import copytree
from string import Template
import subprocess
import time

from ezbake.reverseproxy.thriftapi.ttypes import AuthorizationOperation

"""
Module to setup and tear Nginx configuration
"""

def copyMimeConfig():
    copyfile(gConfig.mimeTemplate,gConfig.mimeConfig)
    
def createMainConfigFile():
    with open(gConfig.mainConfigTemplate) as template:
        template_str = Template(template.read())
        if gConfig.args.ngx_workers < 2:
            nworkers = 2
        else:
            nworkers = gConfig.args.ngx_workers

        config = template_str.substitute(workers=nworkers,
                                         user=gConfig.nginx_worker_username,
                                         logdir=gConfig.logDirectory,
                                         nginxpidfile=gConfig.nginxPidFile,
                                         ezngx_mod_log_prop=gConfig.eznginxmoduleLogProp,
                                         ezconfig_override_dir=gConfig.ezconfig_dir,
                                         root_dir=gConfig.htmlRootDir,
                                         #eznginx_ops_default=AuthorizationOperation._VALUES_TO_NAMES[gConfig.defaultEznginxOps],
                                         #PATCH - remove registration is via WebConsole and not manual nginx.conf files
                                         eznginx_ops_default=' '.join(AuthorizationOperation._VALUES_TO_NAMES.values()),
                                         confdir=gConfig.confdDirectory,
                                         proxy_ssl_ciphers=gConfig.ezproxyciphers,
                                         proxy_ssl_verify_depth=gConfig.max_ca_depth)

        with open(gConfig.mainConfig, 'w') as mainConfigFile:
            mainConfigFile.write(config)
            
def copyManualConfigs(logger):
    if os.path.exists(gConfig.manualDirectory) and os.path.isdir(gConfig.manualDirectory):
        for filename in os.listdir(gConfig.manualDirectory):
            if filename != "servers.conf":
                src = os.path.join(gConfig.manualDirectory,filename)
                dst = os.path.join(gConfig.confdDirectory,filename)
                logger.debug("src: %s  dst: %s" % (src,dst))
                copyfile(src,dst)

def createCAChainFile():
    with open(gConfig.ssl_cafile,'wb') as outfile:
        for root,dirs,files in os.walk(gConfig.ssl_cadir):
            for file in files:
                with open(os.path.join(root, file),'rb') as infile:
                    outfile.write(infile.read())

def createEzNginxModuleLogProps():
    if os.path.isfile(gConfig.eznginxmoduleLogProp):
        return
    #create default log properties configuration for eznginx
    with open(gConfig.eznginxmoduleLogProp,'w') as logprop:
        logprop.write('log4j.rootLogger=INFO, F\n')
        logprop.write('log4j.appender.F=org.apache.log4j.FileAppender\n')
        logprop.write('log4j.appender.F.file=%s\n' % (os.path.join(gConfig.logDirectory,'eznginx_module.log')))
        logprop.write('log4j.appender.F.append=true\n')
        logprop.write('log4j.appender.F.threshold=INFO\n')
        logprop.write('log4j.appender.F.layout=org.apache.log4j.PatternLayout\n')
        logprop.write('log4j.appender.F.layout.conversionPattern=\%d{ISO8601} \%5p [\%X{PID} - \%t] (\%l) - \%m\%n\n')


def createNginxPidFileLink():
    cwd = os.getcwd()
    os.chdir(gConfig.workingDirectory)
    os.symlink(os.path.basename(gConfig.nginxPidFile), 'nginx.pid')
    os.chdir(cwd)


def nginx_basesetup(logger):
    '''
    Creates the working directory for nginx including the config directory,
    basic configuration, conf.d, and log directory.

    This should be called after either nginx_cleanup(), so we can assume the
    directory doesn't already exist.
    '''

    os.makedirs(gConfig.workingDirectory)
    os.makedirs(gConfig.workingDirectory+'/logs')
    for dir in gConfig.ssl_server_certs_dirs:
      os.makedirs(dir)
    subprocess.call(['ln', '-sTf', gConfig.ssl_server_certs_dirs[0], gConfig.ssl_server_certs])

    if not os.path.exists(gConfig.static_contents):
        for dir in gConfig.static_contents_dirs:
          os.makedirs(dir)
        subprocess.call(['ln', '-sTf', gConfig.static_contents_dirs[0], gConfig.static_contents])

    for tmp in ['client_body_temp','fastcgi_temp','proxy_temp','scgi_temp','uwsgi_temp']:
        newdir=os.path.join(gConfig.workingDirectory, 'rxtmp', tmp)
        os.makedirs(newdir)

    if not os.path.isdir(gConfig.logDirectory):
        os.makedirs(gConfig.logDirectory)

    os.makedirs(gConfig.configDirectory)
    os.makedirs(gConfig.confdDirectory)

    copyMimeConfig()
    createMainConfigFile()
    copyManualConfigs(logger)
    createCAChainFile()
    createEzNginxModuleLogProps()
    createNginxPidFileLink()

    open(gConfig.shutdownFile,'w').close()

    try:
        # start nginx - open file handles for stdout & sterr
        max_attempt = 3;
        attempt = 0
        with open(os.path.join(gConfig.logDirectory,'stdout'),'w') as nginxStdout:
            with open(os.path.join(gConfig.logDirectory,'stderr'),'w') as nginxStderr:
                nginxArgs = [gConfig.nginx,'-c',gConfig.mainConfig,'-p',gConfig.workingDirectory]
                nginxInstance = subprocess.Popen(nginxArgs,stdout=nginxStdout,stderr=nginxStderr)
        nginxInstance.wait() #wait for the process to launch the nginx master process
        while True:
            try:
                logger.info("launched nginx. nginx master pid is %d" % get_nginx_master_pid())
                break
            except Exception:
                if attempt >= max_attempt:
                    raise
                attempt += 1
                time.sleep(1)
    except Exception as e:
        logger.exception('Exception in starting the nginx process')
        raise


def get_nginx_master_pid():
    '''
    Finds the master nginx process that was started for the current running instance of the frontend

    @throws IOError if unable to read the nginx pid file created at the start of the running instance
            of the frontend
    '''
    with open(gConfig.nginxPidFile) as pidfile:
        pidstr = pidfile.read()
        return int(pidstr)


def get_all_nginx_master_pids():
    '''
    Find all nginx master pids
    '''
    pids = []
    s = subprocess.Popen('ps -ef | grep "nginx: master process" | grep -v grep', shell=True, stdout=subprocess.PIPE)
    for line in s.stdout:
        pids.append(int(line.split()[1]))
    return pids


def get_desisting_nginx_master_pids():
    '''
    Finds all desisting (nginx master processes that have been sent the QUIT signal, but are still running
    because a child worker process is in the process of shutting down) nginx master processes
    '''
    pids = []
    s = subprocess.Popen('ps -ef | grep "nginx: worker process is shutting down" | grep -v grep', shell=True, stdout=subprocess.PIPE)
    for line in s.stdout:
        mpid = int(line.split()[2])
        if mpid != 1: #worker pid is not fathered by init process
            pids.append(mpid)
    return pids


def get_orphaned_nginx_master_pids():
    '''
    Find all orphaned (nginx master processes that are not shutting down and not tracked by the current
    active nginx pid file) nginx master processes.
    This the intersection of the complement of desisting nginx master processes and all nginx master processes
    '''
    return list(set(get_all_nginx_master_pids()) - set(get_desisting_nginx_master_pids()))


def nginx_cleanup(logger):
    '''
    Finds all master nginx processes on the box and kills them.
    Cleans up the single workindDirectory relevant to this execution
    of the frontend.
    '''
    #get the active master process and stop it
    try:
        master_pid = get_nginx_master_pid()
        logger.info('gracefully shutting down nginx master process %d' % master_pid)
        os.kill(master_pid, signal.SIGQUIT) #graceful shutdown of master process
    except Exception as ex:
        logger.warn('Unable to get the master nginx pid: %s' % str(ex))

    #cleanup master nginx processes that are stale
    desisting_pids = get_desisting_nginx_master_pids()
    orphaned_pids = get_orphaned_nginx_master_pids()
    for pid in orphaned_pids:
        os.kill(pid, signal.SIGTERM) #QUICK shutdown of orphaned master processes
    if orphaned_pids:
        logger.info('force terminated orphaned nginx masters: %s' % str(list(orphaned_pids))[1:-1])
    if desisting_pids:
        logger.info('desisting nginx master pids still waiting on worker shutdown: %s' % str(desisting_pids)[1:-1])

    #recursively remove the workingDirectory
    try:
        rmtree(gConfig.workingDirectory)
    except Exception as e:
        pass


