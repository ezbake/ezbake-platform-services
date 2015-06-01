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



#include <ezbake/nginx/module/NginxAuthModule.h>
#include <ezbake/nginx/connector/NginxSyncConnector.h>
#include <boost/make_shared.hpp>
#include <boost/exception/diagnostic_information.hpp>

#include <log4cxx/propertyconfigurator.h>


namespace ezbake { namespace nginx {


::boost::shared_ptr<connector::NginxAsyncConnector> NginxAuthModule::_connector;
::boost::shared_ptr< ::ezbake::configuration::EZConfiguration> NginxAuthModule::_configuartion;
::std::recursive_mutex NginxAuthModule::_mLock;
::std::set<NginxAuthModule::RequestContext> NginxAuthModule::_pendingRequestContexts;


using ::ezbake::nginx::connector::NginxConnectorInterface;


void NginxAuthModule::configureLog4(ngx_str_t *filePath, ngx_log_t *log) {
    std::ostringstream ss;
    ss << static_cast<unsigned int>(getpid());
    ::log4cxx::MDC::put("PID", ss.str());

    if (filePath->len < 1) {
        ngx_log_error(NGX_LOG_INFO, log, 0,
                "%s: log4j property file path was not set. Skipping initialization of log4cxx", __PRETTY_FUNCTION__);
        return;
    }

    ::std::string logConfigFile(reinterpret_cast<char *>(filePath->data), filePath->len);
    ::log4cxx::PropertyConfigurator::configure(logConfigFile);
}


ngx_int_t NginxAuthModule::initializeModuleCallback(ngx_cycle_t *cycle) {
    ngx_log_debug1(NGX_LOG_DEBUG, cycle->log, 0,
            "%s: module initialized", __PRETTY_FUNCTION__);
    return NGX_OK;
}


ngx_int_t NginxAuthModule::initializeProcessCallback(ngx_cycle_t *cycle)
{
    /* guard check. If we're not in a single process or worker process abort */
    if (ngx_process != NGX_PROCESS_SINGLE && ngx_process != NGX_PROCESS_WORKER) {
        ngx_log_debug1(NGX_LOG_DEBUG_HTTP, cycle->log, 0,
                "%s: module worker initialized: single worker already initialized", __PRETTY_FUNCTION__);
        return NGX_OK;
    }

    ngx_http_ezbake_nginx_auth_main_conf_t *conf;
    conf = (ngx_http_ezbake_nginx_auth_main_conf_t *)ngx_http_cycle_get_module_main_conf(cycle, ngx_http_ezbake_nginx_auth_module);

    if (conf == NULL) {
        ngx_log_error(NGX_LOG_ERR, cycle->log, 0,
                "%s: error in initializing Log4cxx. Couldn't retrieve the main nginx configuration", __PRETTY_FUNCTION__);
        return NGX_ERROR;
    }

    try {
        //configure log4cxx
        configureLog4(&conf->log4jPropertyFilePath, cycle->log);

        //load configuration from defaults
        _configuartion = ::boost::make_shared< ::ezbake::configuration::EZConfiguration>();

        //load configuration overrides
        if (conf->ezConfigOverrideDir.len > 1) {
            _configuartion->loadFromDirectory(::std::string(reinterpret_cast<char *>(conf->ezConfigOverrideDir.data), conf->ezConfigOverrideDir.len), true);
        }

        /*
         * Initialize nginx connector to the security service
         */
        _connector = ::boost::make_shared< ::ezbake::nginx::connector::NginxAsyncConnector>(*_configuartion, CONNECTOR_SERVICENAME);
        _connector->initialize();
    } catch (const ::std::exception &ex) {
        ngx_log_error(NGX_LOG_ERR, cycle->log, 0,
                "%s: error in initializing connector: %s", __PRETTY_FUNCTION__, ex.what());
        return NGX_ERROR;
    } catch (...) {
        //catch everything else to prevent from forwarding to Nginx C code.
        ngx_log_error(NGX_LOG_CRIT, cycle->log, 0,
                "%s: error in initializing connector. Unknown failure!\n%s", __PRETTY_FUNCTION__,
                ::boost::current_exception_diagnostic_information().c_str());
        return NGX_ERROR;
    }

    ngx_log_debug2(NGX_LOG_DEBUG, cycle->log, 0,
            "%s: module worker {PID: %d} initialized", __PRETTY_FUNCTION__, static_cast<unsigned int>(getpid()));

    return NGX_OK;
}


ngx_int_t NginxAuthModule::addEzbVariables(ngx_conf_t *cf)
{
    ngx_http_variable_t  *var, *v;

    for (v = const_cast<ngx_http_variable_t*>(SESSION_VARIABLES); v->name.len; v++) {
        var = ngx_http_add_variable(cf, &v->name, v->flags);
        if (var == NULL) {
            return NGX_ERROR;
        }

        var->get_handler = v->get_handler;
        var->data = v->data;
    }

    return NGX_OK;
}


ngx_int_t NginxAuthModule::getEzbVariable(ngx_http_request_t *r, ngx_http_variable_value_t *v, uintptr_t data)
{
    ngx_str_t  *variable;
    ngx_http_eznginx_auth_ctx_t *ctx;

    //Retrieve module context
    ctx = (ngx_http_eznginx_auth_ctx_t*)ngx_http_get_module_ctx(r, ngx_http_ezbake_nginx_auth_module);
    if (NULL == ctx) {
        ngx_log_debug1(NGX_LOG_DEBUG, r->connection->log, 0,
                "%s: error in getting module context", __PRETTY_FUNCTION__);
        return NGX_ERROR;
    }

    char * variableBase = reinterpret_cast<char *>(&(ctx->vars));
    variable = reinterpret_cast<ngx_str_t *>(variableBase + data);

    //Retrieve variable
    if (variable->len > 1) {
        v->len = static_cast<unsigned>(variable->len);
        v->data = (u_char*)ngx_pnalloc(r->pool, v->len);
        if (NULL == v->data) {
            return NGX_ERROR;
        }
        ngx_memcpy(v->data, variable->data, v->len);

        v->valid = 1;
        v->no_cacheable = 0;
        v->not_found = 0;

    } else {
        //variable is not ready - it has not been set. Indicate not found.
        v->len = 0;
        v->not_found = 1;
    }

    return NGX_OK;
}


void* NginxAuthModule::createMainConfiguration(ngx_conf_t *cf)
{
    ngx_http_ezbake_nginx_auth_main_conf_t *conf;

    conf = (ngx_http_ezbake_nginx_auth_main_conf_t *)ngx_pcalloc(cf->pool, sizeof(ngx_http_ezbake_nginx_auth_main_conf_t));
    if (conf == NULL) {
        return NULL;
    }

    return conf;
}


char * NginxAuthModule::initMainConfiguration(ngx_conf_t *cf, void *parent)
{
    ngx_http_ezbake_nginx_auth_main_conf_t *conf = (ngx_http_ezbake_nginx_auth_main_conf_t *)parent;

    if (conf->log4jPropertyFilePath.len < 1) {
        ngx_conf_log_error(NGX_LOG_INFO, cf, 0, "%s: eznginx_log_props configuration not specified", __PRETTY_FUNCTION__);
    }

    return NGX_CONF_OK;
}


void * NginxAuthModule::initLocationConfiguration(ngx_conf_t *cf) {
    ngx_http_eznginx_auth_loc_conf_t *conf;

    conf = (ngx_http_eznginx_auth_loc_conf_t *)ngx_pcalloc(cf->pool, sizeof(ngx_http_eznginx_auth_loc_conf_t));
    if (conf == NULL) {
        return NULL;
    }

    return conf;
}


char * NginxAuthModule::mergeLocationConfiguration(ngx_conf_t *cf, void *parent, void *child) {
    ngx_http_eznginx_auth_loc_conf_t *prev = (ngx_http_eznginx_auth_loc_conf_t *)parent;
    ngx_http_eznginx_auth_loc_conf_t *conf = (ngx_http_eznginx_auth_loc_conf_t *)child;

    ngx_conf_merge_bitmask_value(conf->authOperations, prev->authOperations, 0);

    return NGX_CONF_OK;
}


ngx_int_t NginxAuthModule::initialize(ngx_conf_t *cf)
{
    ngx_http_handler_pt *h;
    ngx_http_core_main_conf_t *cmcf;

    cmcf = (ngx_http_core_main_conf_t*)ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);

    /*
     * Register our authentication module to be called in the PREACCESS phase of NGINX.
     * Unlike the ACCESS phase, the PREACCESS phase of NGINX doesn't use the 'satisfy' directive.
     */
    //h = (ngx_http_handler_pt*)ngx_array_push(&cmcf->phases[NGX_HTTP_ACCESS_PHASE].handlers); //uses satisfy directive
    h = (ngx_http_handler_pt*)ngx_array_push(&cmcf->phases[NGX_HTTP_PREACCESS_PHASE].handlers); //doesn't use satisfy directive

    if (NULL == h) {
        ngx_log_error(NGX_LOG_CRIT, cf->log, 0,
                "%s: error in adding authentication handler to Nginx pre-access phase", __PRETTY_FUNCTION__);
        return NGX_ERROR;
    }

    *h = handleAuthenticationRequest;

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, cf->log, 0,
            "%s: registered auth handler in the NGX_HTTP_PREACCESS_PHASE", __PRETTY_FUNCTION__);

    return NGX_OK;
}


ngx_int_t NginxAuthModule::handleAuthenticationRequest(ngx_http_request_t *r)
{
    /* deny all connections not via ssl */
    if (NULL == r->connection->ssl) {
        ngx_log_error(NGX_LOG_WARN, r->connection->log, 0, "Denied non-secure connection attempt.");
        return NGX_HTTP_FORBIDDEN;
    }

    /* check if request location has specified authentication operation */
    ngx_http_eznginx_auth_loc_conf_t *lconf =
            (ngx_http_eznginx_auth_loc_conf_t*)ngx_http_get_module_loc_conf(r,
                                                                            ngx_http_ezbake_nginx_auth_module);
    if (NULL == lconf) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                "%s: error in reading location configuration", __PRETTY_FUNCTION__);
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    if (!(lconf->authOperations)) {
        ngx_log_error(NGX_LOG_ALERT, r->connection->log, 0, "Denying connection. Authenication opertation not configured");
        return NGX_HTTP_FORBIDDEN;
    }

    ngx_http_eznginx_auth_ctx_t *ctx =
            (ngx_http_eznginx_auth_ctx_t*)ngx_http_get_module_ctx(r, ngx_http_ezbake_nginx_auth_module);

    if (NULL == ctx) {
        /* this is a new attempt at authenticating the connection request */
        ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                "%s: creating new auth context", __PRETTY_FUNCTION__);

        ctx = (ngx_http_eznginx_auth_ctx_t*)ngx_pcalloc(r->pool, sizeof(ngx_http_eznginx_auth_ctx_t));
        if (NULL == ctx) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                    "%s: error in creating new auth context", __PRETTY_FUNCTION__);
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }

        //save a reference to the active http request for reverse lookup
        ctx->initiatingRequest = r;

        //register the new context with the http request
        ngx_http_set_ctx(r, ctx, ngx_http_ezbake_nginx_auth_module);
        ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                "%s: registerd new auth context with http_request", __PRETTY_FUNCTION__);

        //Retrieve peer SSL certificate from connection
        ctx->cert = SSL_get_peer_certificate(r->connection->ssl->connection);
        if (NULL == ctx->cert) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                   "%s: Unable to get peer certificate. Declining to verify with EzSecurity!", __PRETTY_FUNCTION__);
            return NGX_HTTP_FORBIDDEN;
        }

        //initialize dispatch params
        ctx->authenticationDispatch.log = r->connection->log;
        ctx->authenticationDispatch.data = new (ngx_palloc(r->pool, sizeof(RequestContext))) RequestContext(ctx);
        ctx->authenticationDispatch.handler = dispatchEventHandler;
        ctx->state = UNINITIALIZED;
    }

    return verifyWithEzSecurity(ctx);
}


std::string NginxAuthModule::getFormattedName(X509_NAME *name)
{
    char *dataStart = NULL, *nameString = NULL;
    long nameLength = 0;

    BIO *bio = BIO_new(BIO_s_mem());

    // Now, put the subject line into the BIO.
    X509_NAME_print_ex(bio, name, 0, (ASN1_STRFLGS_RFC2253 | \
                                      XN_FLAG_SEP_CPLUS_SPC | \
                                      XN_FLAG_DN_REV | \
                                      XN_FLAG_FN_SN | \
                                      XN_FLAG_DUMP_UNKNOWN_FIELDS));// | ASN1_STRFLGS_ESC_QUOTE));

    // Obtain a reference to the data and copy out
    // just the length of the data.
    nameLength = BIO_get_mem_data(bio, &dataStart);

    nameString = (char *) malloc(nameLength + 1);
    memset(nameString, 0x00, nameLength + 1);
    memcpy(nameString, dataStart, nameLength);

    BIO_free(bio);

    std::string retVal = nameString;
    free(nameString);
    return retVal;
}


void NginxAuthModule::getCertNames(X509* cert, ::ezbake::base::thrift::X509Info& x509Info) {
    X509_NAME *sname = X509_get_subject_name(cert);
    x509Info.__set_subject(sname ? getFormattedName(sname) : "(none)");

    X509_NAME *iname = X509_get_issuer_name(cert);
    x509Info.__set_issuer(iname ? getFormattedName(iname) : "(none)");
}


ngx_int_t NginxAuthModule::verifyWithEzSecurity(ngx_http_eznginx_auth_ctx_t *ctx)
{
    int rc = NGX_AGAIN; //repeat handler phase again

    ngx_log_t *log = ctx->initiatingRequest->connection->log;

    //retrieve names from cert
    ::ezbake::base::thrift::X509Info x509Info;
    getCertNames(ctx->cert, x509Info);

    try {
        switch(ctx->state) {
            case UNINITIALIZED:
            {
                rc = startVerfication(ctx, x509Info);
                break;
            }

            case PENDING:
                //restart nginx poll
                ngx_add_timer(&ctx->authenticationDispatch,
                        _configuartion->getLong(CONNECTOR_POLL_PERIOD_KEY, CONNECTOR_POLL_PERIOD));
                break;

            case PASSED:
                rc = NGX_OK;
                ngx_log_error(NGX_LOG_ALERT, log, 0,
                              "Successfully authenticated connection for SUBJECT: \"%s\" ISSUER: \"%s\" VALIDATED DN: \"%V\"",
                              x509Info.subject.c_str(), x509Info.issuer.c_str(), &ctx->vars.ezb_user_info);
                break;

            case FAILED:
                rc = NGX_HTTP_FORBIDDEN;
                ngx_log_error(NGX_LOG_ALERT, log, 0,
                           "FAILED authentication with security service. SUBJECT: \"%s\" ISSUER: \"%s\"",
                           x509Info.subject.c_str(), x509Info.issuer.c_str());
                break;

            case ERROR:
                rc = NGX_HTTP_INTERNAL_SERVER_ERROR;
                ngx_log_error(NGX_LOG_ALERT, log, 0,
                           "FAILED authentication with security service. SUBJECT: \"%s\" ISSUER: \"%s\"",
                           x509Info.subject.c_str(), x509Info.issuer.c_str());
                break;

            default:
                rc = NGX_HTTP_INTERNAL_SERVER_ERROR;
                ngx_log_error(NGX_LOG_ALERT, log, 0,
                           "%s: Invalid authentication state: %d", ctx->state);
                break;
        }
    } catch (const ::std::exception &ex) {
        ngx_log_error(NGX_LOG_ERR, log, 0,
                "%s: error in authenticating user: %s", __PRETTY_FUNCTION__, ex.what());
        rc = NGX_HTTP_INTERNAL_SERVER_ERROR;
    } catch (...) {
        //catch unhandled exceptions to prevent from forwarding to Nginx C code.
        ngx_log_error(NGX_LOG_CRIT, log, 0,
                "%s: critical error in authenticating user. Unknown failure!\n%s", __PRETTY_FUNCTION__,
                ::boost::current_exception_diagnostic_information().c_str());
        rc = NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    return rc;
}


ngx_int_t NginxAuthModule::startVerfication(ngx_http_eznginx_auth_ctx_t *ctx,
        const ::ezbake::base::thrift::X509Info& x509) {
    /*
     * This is a new authentication request for the context.
     * Check if authentication data of user is available with the security client cache
     */

    int rc = NGX_AGAIN;
    ngx_log_t *log = ctx->initiatingRequest->connection->log;

    ngx_http_eznginx_auth_loc_conf_t *locationConfiguration =
            (ngx_http_eznginx_auth_loc_conf_t*)ngx_http_get_module_loc_conf(ctx->initiatingRequest,
                                                                            ngx_http_ezbake_nginx_auth_module);

    if (NULL == locationConfiguration) {
        ngx_log_error(NGX_LOG_ERR, log, 0,
                "%s: error in obtaining location configuration", __PRETTY_FUNCTION__);
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }


    RequestContext *reqContext = reinterpret_cast<RequestContext*>(ctx->authenticationDispatch.data);

    if (locationConfiguration->authOperations & AUTH_OPS_USER_INFO) {
        try {
            ::boost::optional<NginxConnectorInterface::AuthenticationData::UserInfo>  userInfoAuthData =
                    _connector->authenticateUserInfo(::boost::bind(&NginxAuthModule::handleUserInfoResponse,
                                                                   _1, _2, *reqContext), x509);
            if (userInfoAuthData) {
                //update ezb variables with cached authenticated data
                updateEzbUserInfoForContext(ctx, *userInfoAuthData);
            } else {
                //indicate authentication pending
                ctx->authOperations |= AUTH_OPS_USER_INFO;
                addRequestContext(*reqContext);
            }
        } catch (const ::ezbake::base::thrift::EzSecurityTokenException& ex) {
            ctx->state = ERROR;
            rc = NGX_HTTP_INTERNAL_SERVER_ERROR;
            removeRequestContext(*reqContext);
            ngx_log_error(NGX_LOG_ERR, log, 0,
                    "%s: error in authenticating UserInfo for user: %s", __PRETTY_FUNCTION__, ex.message.c_str());
        }
    }

    if (locationConfiguration->authOperations & AUTH_OPS_USER_JSON) {
        try {
            ::boost::optional<NginxConnectorInterface::AuthenticationData::UserJson> userJsonAuthData =
                    _connector->authenticateUserJson(::boost::bind(&NginxAuthModule::handleUserJsonResponse,
                                                                   _1, _2, *reqContext), x509);
            if (userJsonAuthData) {
                //update ezb variables with cached authenticated data
                updateEzbUserJsonForContext(ctx, *userJsonAuthData);
            } else {
                //indicate authorized pending
                ctx->authOperations |= AUTH_OPS_USER_JSON;
                addRequestContext(*reqContext);
            }
        } catch (const ::ezbake::base::thrift::EzSecurityTokenException& ex) {
            ctx->state = ERROR;
            rc = NGX_HTTP_INTERNAL_SERVER_ERROR;
            removeRequestContext(*reqContext);
            ngx_log_error(NGX_LOG_ERR, log, 0,
                    "%s: error in authenticating UserJson for user: %s", __PRETTY_FUNCTION__, ex.message.c_str());
        }
    }

    if (ctx->authOperations) {
        //Not all user authenticaiton data available in security client cache.
        //Async call to security service already dispatched

        ctx->state = PENDING;

        //start nginx poll
        ngx_add_timer(&ctx->authenticationDispatch,
                _configuartion->getLong(CONNECTOR_POLL_PERIOD_KEY, CONNECTOR_POLL_PERIOD));
    } else {
        //User authorized with cache
        rc = NGX_OK;
        ctx->state = PASSED;
        ngx_log_error(NGX_LOG_ALERT, log, 0,
                      "Successfully authenticated connection (c) for SUBJECT: \"%s\" ISSUER: \"%s\" VALIDATED DN: \"%V\"",
                      x509.subject.c_str(), x509.issuer.c_str(), &ctx->vars.ezb_user_info);
    }

    return rc;
}


void NginxAuthModule::handleUserInfoResponse(const boost::shared_ptr< ::std::exception >& err,
        const ::boost::shared_ptr<NginxConnectorInterface::AuthenticationData::UserInfo>& authData,
        const RequestContext& reqContext) {
    if (isRequestContextPending(reqContext)) {
        ngx_http_eznginx_auth_ctx_t *ctx = reqContext.context();

        //clear pending authentication
        ctx->authOperations &= ~AUTH_OPS_USER_INFO;

        if (err) {
            ngx_log_error(NGX_LOG_ERR, ctx->initiatingRequest->connection->log, 0,
                          "Error in authorizing with user info - see eznginxmodule logs");
            ctx->state = FAILED;
            removeRequestContext(reqContext);
            return;
        }

        ngx_log_debug(NGX_LOG_DEBUG, ctx->initiatingRequest->connection->log, 0,
                              "Success authorizing with user info");

        updateEzbUserInfoForContext(ctx, *authData);
    }
}


void NginxAuthModule::handleUserJsonResponse(const boost::shared_ptr< ::std::exception >& err,
        const ::boost::shared_ptr<NginxConnectorInterface::AuthenticationData::UserJson>& authData,
        const RequestContext& reqContext) {
    if (isRequestContextPending(reqContext)) {
        ngx_http_eznginx_auth_ctx_t *ctx = reqContext.context();

        //clear pending authentication
        ctx->authOperations &= ~AUTH_OPS_USER_JSON;

        if (err) {
            ngx_log_error(NGX_LOG_ERR, ctx->initiatingRequest->connection->log, 0,
                          "Error in authorizing with user json - see eznginxmodule logs");
            ctx->state = FAILED;
            removeRequestContext(reqContext);
            return;
        }

        ngx_log_debug(NGX_LOG_DEBUG, ctx->initiatingRequest->connection->log, 0,
                      "Success authorizing with user json");

        updateEzbUserJsonForContext(ctx, *authData);
    }
}


void NginxAuthModule::updateEzbUserInfoForContext(ngx_http_eznginx_auth_ctx_t *ctx,
        const NginxConnectorInterface::AuthenticationData::UserInfo& authData) {

    ngx_str_t nstr;
    ngx_pool_t *pool = ctx->initiatingRequest->pool;

    nstr = { sizeof(u_char) * authData.proxyTokenString.length(),
             (u_char *) authData.proxyTokenString.c_str() };
    ctx->vars.ezb_user_info.len = nstr.len;
    ctx->vars.ezb_user_info.data = ngx_pstrdup(pool, &nstr);

    /* Update Nginx variable $ezb_remote_user */
    nstr = { sizeof(u_char) * authData.userDN.length(),
        (u_char *) authData.userDN.c_str() };
    ctx->vars.ezb_remote_user.len = nstr.len;
    ctx->vars.ezb_remote_user.data = ngx_pstrdup(pool, &nstr);

    nstr = { sizeof(u_char) * authData.proxyTokenSignature.length(),
             (u_char *) authData.proxyTokenSignature.c_str() };
    ctx->vars.ezb_user_signature.len = nstr.len;
    ctx->vars.ezb_user_signature.data = ngx_pstrdup(pool, &nstr);
}


void NginxAuthModule::updateEzbUserJsonForContext(ngx_http_eznginx_auth_ctx_t *ctx,
        const NginxConnectorInterface::AuthenticationData::UserJson& authData) {

    ngx_str_t nstr;
    ngx_pool_t *pool = ctx->initiatingRequest->pool;

    nstr = { sizeof(u_char) * authData.jsonString.length(),
             (u_char *) authData.jsonString.c_str() };
    ctx->vars.ezb_user_info_json.len = nstr.len;
    ctx->vars.ezb_user_info_json.data = ngx_pstrdup(pool, &nstr);

    nstr = { sizeof(u_char) * authData.jsonSignature.length(),
             (u_char *) authData.jsonSignature.c_str() };
    ctx->vars.ezb_user_info_json_signature.len = nstr.len;
    ctx->vars.ezb_user_info_json_signature.data = ngx_pstrdup(pool, &nstr);
}


void NginxAuthModule::dispatchEventHandler(ngx_event_t *ev) {
    RequestContext* reqContext = reinterpret_cast<RequestContext*>(ev->data);
    ngx_http_eznginx_auth_ctx_t* ctx = reqContext->context();

    try {
        switch (ctx->state) {
            case PENDING:
                if (!(ctx->authOperations)) {
                    //authentication complete. Wake up PASSED phase
                    ctx->state = PASSED;
                    removeRequestContext(*reqContext);
                    ngx_http_core_run_phases(ctx->initiatingRequest);
                } else {
                    if (++ctx->pollTry >=
                            _configuartion->getInt(CONNECTOR_POLL_MAX_TRIES_KEY, CONNECTOR_POLL_MAX_TRIES)) {
                        ngx_log_error(NGX_LOG_ERR, ctx->initiatingRequest->connection->log, 0,
                                      "Authenticating user taking longer than expected. Aborting.");
                        ctx->state = ERROR;
                        ngx_http_core_run_phases(ctx->initiatingRequest);
                        resetPendingRequestContexts(*reqContext);
                    } else {
                        //restart nginx poll
                        ngx_add_timer(&ctx->authenticationDispatch,
                                _configuartion->getLong(CONNECTOR_POLL_PERIOD_KEY, CONNECTOR_POLL_PERIOD));
                    }
                }
                break;

            default:
                //wake up current phase to handle authentication response
                ngx_http_core_run_phases(ctx->initiatingRequest);
                break;
        }
    } catch (const ::std::exception &ex) {
        ngx_log_error(NGX_LOG_ERR, ctx->initiatingRequest->connection->log, 0,
                "%s: error in handling dispatch event for context. %s", __PRETTY_FUNCTION__, ex.what());
    } catch (...) {
        //catch everything else to prevent from forwarding to Nginx C code.
        ngx_log_error(NGX_LOG_CRIT, ctx->initiatingRequest->connection->log, 0,
                "%s: error in handling dispatch event for context. Unknown failure!\n%s", __PRETTY_FUNCTION__,
                ::boost::current_exception_diagnostic_information().c_str());
    }
}


void NginxAuthModule::resetPendingRequestContexts(const RequestContext& initiatingContext) {
    bool reinitializeConnector = false;

    {//synchronized
        std::lock_guard<std::recursive_mutex> lock(_mLock);
        if (_pendingRequestContexts.end() != _pendingRequestContexts.find(initiatingContext)) {
            _pendingRequestContexts.erase(_pendingRequestContexts.begin(), _pendingRequestContexts.end());
            reinitializeConnector = true;
        }
    }

    if (reinitializeConnector &&
        _configuartion->getBoolean(CONNECTOR_REINITIALIZE_ON_PENDING_REQUEST_RESET, true)) {
        _connector->initialize();
    }
}


void NginxAuthModule::removeRequestContext(const RequestContext& context) {
    {//synchronized
        std::lock_guard<std::recursive_mutex> lock(_mLock);
        _pendingRequestContexts.erase(context);
    }
}


void NginxAuthModule::addRequestContext(const RequestContext& context) {
    {//synchronized
        std::lock_guard<std::recursive_mutex> lock(_mLock);
        _pendingRequestContexts.insert(context);
    }
}


}} //::ezbake::nginx
