#!/usr/bin/python
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
import imp
import logging

# setup logging
logpath = os.path.join(os.environ.get('OPENSHIFT_LOG_DIR', '/tmp'), 'python.log')
logging.basicConfig(filename=logpath, level=logging.INFO)
log = logging.getLogger()

# openshift's setup
virtenv = os.environ.get('OPENSHIFT_PYTHON_DIR', '/usr/bin/python') + '/virtenv/'
virtualenv = os.path.join(virtenv, 'bin/activate_this.py')

# dynamically load application's WSGI
wsgi_filepath = os.path.join(
    os.environ.get('OPENSHIFT_REPO_DIR', '.'),
    os.environ.get('EZBAKE_WSGI_ENTRY_POINT', 'ezbake/wsgi.py'))
try:
    wsgi_filename = os.path.basename(wsgi_filepath).split('.')[0]
    wsgi_module = imp.load_source(wsgi_filename, wsgi_filepath)
    application = wsgi_module.application
except Exception, e:
    log.exception("==== UNABLE TO GET ENTRY TO WSGI ===")
    log.error("EZBAKE_WSGI_ENTRY_POINT='%s'" % wsgi_filepath)
    raise
