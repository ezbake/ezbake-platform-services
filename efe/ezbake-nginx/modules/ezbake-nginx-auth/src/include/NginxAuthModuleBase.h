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


#ifndef EZBAKE_NGINX_NGINXAUTHMODULEBASE_H_
#define EZBAKE_NGINX_NGINXAUTHMODULEBASE_H_

//extern nginx c headers
extern "C" {
#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_event.h>
}

//Declare our entry into nginx - this is defined in our nginx module file
extern ngx_module_t ngx_http_ezbake_nginx_auth_module;


namespace ezbake { namespace nginx {


class NginxAuthModuleBase {
public: //types
    typedef ngx_int_t (*ngx_module_entry_init_master_func)(ngx_log_t *log);
    typedef ngx_int_t (*ngx_module_entry_init_func)(ngx_cycle_t *cycle);
    typedef void (*ngx_module_entry_exit_func)(ngx_cycle_t *cycle);

public: //constant variables
    static const ngx_command_t COMMANDS[];
    static const ngx_http_module_t MODULE_CONTEXT;

    //Function pointers to entry points in Nginx module declaration.
    static const ngx_module_entry_init_master_func INIT_MASTER_HANDLER;
    static const ngx_module_entry_init_func INIT_MODULE_HANDLER;
    static const ngx_module_entry_init_func INIT_PROCESS_HANDLER;
    static const ngx_module_entry_init_func INIT_THREAD_HANDLER;
    static const ngx_module_entry_exit_func EXIT_THREAD_HANDLER;
    static const ngx_module_entry_exit_func EXIT_PROCESS_HANDLER;
    static const ngx_module_entry_exit_func EXIT_MASTER_HANDLER;
};

}} // ezbake::nginx

#endif /* EZBAKE_NGINX_NGINXAUTHMODULEBASE_H_ */
