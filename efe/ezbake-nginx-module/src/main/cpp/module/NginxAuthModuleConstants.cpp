/*   Copyright (C) 2013-2014 Computer Sciences Corporation
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



#include <ezbake/nginx/module/NginxAuthModule.h>

using namespace ezbake::nginx;

/**
 * Initialize our static constants
 */


const ngx_conf_bitmask_t  NginxAuthModule::OPERATION_MASKS[] = {
    { ngx_string("USER_INFO"), NginxAuthModule::AUTH_OPS_USER_INFO },
    { ngx_string("USER_JSON"), NginxAuthModule::AUTH_OPS_USER_JSON },
    { ngx_null_string, 0 }
};


const ngx_command_t NginxAuthModuleBase::COMMANDS[] = {

    {
        ngx_string("eznginx_log_props"),
        NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_MAIN_CONF_OFFSET,
        offsetof(NginxAuthModule::ngx_http_ezbake_nginx_auth_main_conf_t, log4jPropertyFilePath),
        NULL
    },

    {
        ngx_string("ezconfig_override_dir"),
        NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_MAIN_CONF_OFFSET,
        offsetof(NginxAuthModule::ngx_http_ezbake_nginx_auth_main_conf_t, ezConfigOverrideDir),
        NULL
    },

    {
        ngx_string("eznginx_ops"),
        NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_1MORE,
        ngx_conf_set_bitmask_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(NginxAuthModule::ngx_http_eznginx_auth_loc_conf_t, authOperations),
        (void *)&NginxAuthModule::OPERATION_MASKS
    },

    ngx_null_command
};


const ngx_http_module_t NginxAuthModuleBase::MODULE_CONTEXT = {
    NginxAuthModule::addEzbVariables,             /* preconfiguration */
    NginxAuthModule::initialize,                  /* postconfiguration */
    NginxAuthModule::createMainConfiguration,     /* create main configuration */
    NginxAuthModule::initMainConfiguration,       /* init main configuration */
    NULL,                                           /* create server configuration */
    NULL,                                           /* merge server configuration */
    NginxAuthModule::initLocationConfiguration,   /* create location configuration */
    NginxAuthModule::mergeLocationConfiguration   /* merge location configuration */
};


const NginxAuthModuleBase::ngx_module_entry_init_master_func NginxAuthModuleBase::INIT_MASTER_HANDLER = NULL;
const NginxAuthModuleBase::ngx_module_entry_init_func NginxAuthModuleBase::INIT_MODULE_HANDLER = NginxAuthModule::initializeModuleCallback;
const NginxAuthModuleBase::ngx_module_entry_init_func NginxAuthModuleBase::INIT_PROCESS_HANDLER = NginxAuthModule::initializeProcessCallback;
const NginxAuthModuleBase::ngx_module_entry_init_func NginxAuthModuleBase::INIT_THREAD_HANDLER = NULL;
const NginxAuthModuleBase::ngx_module_entry_exit_func NginxAuthModuleBase::EXIT_THREAD_HANDLER = NULL;
const NginxAuthModuleBase::ngx_module_entry_exit_func NginxAuthModuleBase::EXIT_PROCESS_HANDLER = NULL;
const NginxAuthModuleBase::ngx_module_entry_exit_func NginxAuthModuleBase::EXIT_MASTER_HANDLER = NULL;


const ngx_http_variable_t  NginxAuthModule::SESSION_VARIABLES[] = {

    { ngx_string("ezb_remote_user"), NULL, NginxAuthModule::getEzbVariable,
      (uintptr_t)offsetof(NginxAuthModule::ngx_http_eznginx_auth_vars_t, ezb_remote_user),
      NGX_HTTP_VAR_CHANGEABLE | NGX_HTTP_VAR_NOCACHEABLE, 0 },

    { ngx_string("ezb_verified_user_info"), NULL, NginxAuthModule::getEzbVariable,
      (uintptr_t)offsetof(NginxAuthModule::ngx_http_eznginx_auth_vars_t, ezb_user_info),
      NGX_HTTP_VAR_CHANGEABLE | NGX_HTTP_VAR_NOCACHEABLE, 0 },

    { ngx_string("ezb_verified_signature"), NULL, NginxAuthModule::getEzbVariable,
      (uintptr_t)offsetof(NginxAuthModule::ngx_http_eznginx_auth_vars_t, ezb_user_signature),
      NGX_HTTP_VAR_CHANGEABLE | NGX_HTTP_VAR_NOCACHEABLE, 0 },

    { ngx_string("ezb_user_info_json"), NULL, NginxAuthModule::getEzbVariable,
      (uintptr_t)offsetof(NginxAuthModule::ngx_http_eznginx_auth_vars_t, ezb_user_info_json),
      NGX_HTTP_VAR_CHANGEABLE | NGX_HTTP_VAR_NOCACHEABLE, 0 },

    { ngx_string("ezb_user_info_json_signature"), NULL, NginxAuthModule::getEzbVariable,
      (uintptr_t)offsetof(NginxAuthModule::ngx_http_eznginx_auth_vars_t, ezb_user_info_json_signature),
      NGX_HTTP_VAR_CHANGEABLE | NGX_HTTP_VAR_NOCACHEABLE, 0 },

    { ngx_null_string, NULL, NULL, 0, 0, 0 }
};

const ngx_msec_t NginxAuthModule::CONNECTOR_POLL_PERIOD = 600;//2*NGX_TIMER_LAZY_DELAY
const int NginxAuthModule::CONNECTOR_POLL_MAX_TRIES = 50;
const ::std::string NginxAuthModule::CONNECTOR_SERVICENAME = "efe";
const ::std::string NginxAuthModule::CONNECTOR_POLL_PERIOD_KEY = CONNECTOR_SERVICENAME + ".nginx.auth.poll.period";
const ::std::string NginxAuthModule::CONNECTOR_POLL_MAX_TRIES_KEY = CONNECTOR_SERVICENAME + ".nginx.auth.poll.max.tries";
const ::std::string NginxAuthModule::CONNECTOR_REINITIALIZE_ON_PENDING_REQUEST_RESET = CONNECTOR_SERVICENAME + ".ezgnix.auth.enable.reinit.on.reset";
