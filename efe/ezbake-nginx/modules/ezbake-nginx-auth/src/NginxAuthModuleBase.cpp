/*   Copyright (C) 2013-2015 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */


#include "./include/NginxAuthModuleBase.h"

/**
 * Nginx main entry point
 */
ngx_module_t ngx_http_ezbake_nginx_auth_module = {
    NGX_MODULE_V1,
    (void*)&ezbake::nginx::NginxAuthModuleBase::MODULE_CONTEXT,               /* module context */
    const_cast<ngx_command_t*>(ezbake::nginx::NginxAuthModuleBase::COMMANDS), /* module directives */
    NGX_HTTP_MODULE,                                                          /* module type */
    ezbake::nginx::NginxAuthModuleBase::INIT_MASTER_HANDLER,                  /* init master */
    ezbake::nginx::NginxAuthModuleBase::INIT_MODULE_HANDLER,                  /* init module */
    ezbake::nginx::NginxAuthModuleBase::INIT_PROCESS_HANDLER,                 /* init process */
    ezbake::nginx::NginxAuthModuleBase::INIT_THREAD_HANDLER,                  /* init thread */
    ezbake::nginx::NginxAuthModuleBase::EXIT_THREAD_HANDLER,                  /* exit thread */
    ezbake::nginx::NginxAuthModuleBase::EXIT_PROCESS_HANDLER,                 /* exit process */
    ezbake::nginx::NginxAuthModuleBase::EXIT_MASTER_HANDLER,                  /* exit master */
    NGX_MODULE_V1_PADDING
};

