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



#ifndef EZBAKE_NGINX_MODULE_NGINXAUTHMODULE_H_
#define EZBAKE_NGINX_MODULE_NGINXAUTHMODULE_H_

#include <ezbake/nginx/connector/NginxAsyncConnector.h>
#include <boost/shared_ptr.hpp>

#include "NginxAuthModuleBase.h"

namespace ezbake { namespace nginx {

class NginxAuthModule : public NginxAuthModuleBase {
public: //types
    /*
     * Structure to hold main configuration variables
     */
    typedef struct {
        ngx_str_t   log4jPropertyFilePath;
        ngx_str_t   ezConfigOverrideDir;
    } ngx_http_ezbake_nginx_auth_main_conf_t;

    /*
     * Structure to hold location configuration variables
     */
    typedef struct {
        ngx_uint_t  authOperations;
    } ngx_http_eznginx_auth_loc_conf_t;

    /*
     * Authentication delegate dispatch state
     */
    typedef enum {
        UNINITIALIZED = 0,
        PENDING,
        PASSED,
        FAILED,
        ERROR
    }AuthenticationDispatchState;

    /*
     * Structure to hold our authentication variables for session
     */
    typedef struct {
        ngx_str_t   ezb_remote_user;
        ngx_str_t   ezb_user_info;
        ngx_str_t   ezb_user_signature;
        ngx_str_t   ezb_user_info_json;
        ngx_str_t   ezb_user_info_json_signature;
    } ngx_http_eznginx_auth_vars_t;


    /*
     * Structure for our authentication context.
     * A context is created for each unique connection.
     */
    typedef struct {
        ngx_http_request_t *initiatingRequest;  //reference to the original http request that initiated authentication access
        ngx_http_eznginx_auth_vars_t vars;      //location to store authentication variables from security service
        X509 *cert;                             //peer certificate associated with the http request
        ngx_event_t authenticationDispatch;     //event used to monitor the authentication dispatch progress
        AuthenticationDispatchState state;      //current state of the authentication dispatch
        ngx_int_t pollTry;
        ngx_uint_t authOperations;
    } ngx_http_eznginx_auth_ctx_t;


    class RequestContext {
    public:
        RequestContext(ngx_http_eznginx_auth_ctx_t *ctx) :
            _ctx(ctx),
            _timestamp((boost::posix_time::microsec_clock::universal_time() -
                        boost::posix_time::from_time_t(0)).total_milliseconds())
        {}

        virtual ~RequestContext() {}

        inline bool operator==(const RequestContext& rhs) const {
            return ((_ctx == rhs._ctx) && (_timestamp == rhs._timestamp));
        }

        inline bool operator!=(const RequestContext& rhs) const {
            return !(*this == rhs);
        }

        inline bool operator<(const RequestContext& rhs) const {
            return (_timestamp < rhs._timestamp);
        }

        ngx_http_eznginx_auth_ctx_t* context() const {
            return _ctx;
        }

        uint64_t creationTime() const {
            return _timestamp;
        }

    private:
        ngx_http_eznginx_auth_ctx_t *_ctx;
        uint64_t _timestamp;
    };


public: //constant variables - local
    static const int AUTH_OPS_USER_INFO = 0x00000001;
    static const int AUTH_OPS_USER_JSON = 0x00000002;

    static const ngx_http_variable_t  SESSION_VARIABLES[];
    static const ngx_msec_t CONNECTOR_POLL_PERIOD;
    static const int CONNECTOR_POLL_MAX_TRIES;
    static const ::std::string CONNECTOR_SERVICENAME;
    static const ::std::string CONNECTOR_POLL_PERIOD_KEY;
    static const ::std::string CONNECTOR_POLL_MAX_TRIES_KEY;
    static const ::std::string CONNECTOR_REINITIALIZE_ON_PENDING_REQUEST_RESET;
    static const ngx_conf_bitmask_t OPERATION_MASKS[];


public: //functions

    /*
     * Nginx callback that populates the request EZB variable
     */
    static ngx_int_t getEzbVariable(ngx_http_request_t *r, ngx_http_variable_value_t *v, uintptr_t data);

    /*
     * Nginx callback that finalizes the initialization of the module
     */
    static ngx_int_t initializeModuleCallback(ngx_cycle_t *cycle);

    /*
     * Nginx callback that finalizes the initalization of a worker process
     */
    static ngx_int_t initializeProcessCallback(ngx_cycle_t *cycle);

    /*
     * Initializes nginx log4 logging
     */
    static void configureLog4(ngx_str_t *filePath, ngx_log_t *log);

    /*
     * Nginx callback to make our EZB variables known to Nginx
     */
    static ngx_int_t addEzbVariables(ngx_conf_t *cf);

    /*
     * Nginx callback to create our main configuration
     */
    static void * createMainConfiguration(ngx_conf_t *cf);

    /*
     * Nginx callback to initialize main configuration
     */
    static char * initMainConfiguration(ngx_conf_t *cf, void *parent);

    /*
     * Nginx callback to initialize location configuration
     */
    static void * initLocationConfiguration(ngx_conf_t *cf);

    /*
     * Nginx callback to merge location configuration
     */
    static char * mergeLocationConfiguration(ngx_conf_t *cf, void *parent, void *child);

    /*
     * Nginx callback to initialize our module
     */
    static ngx_int_t initialize(ngx_conf_t *cf);

    /*
     * Nginx callback requesting for authentication with the security service
     */
    static ngx_int_t handleAuthenticationRequest(ngx_http_request_t *r);


    /*
     * Nginx callback to handle events
     */
    static void dispatchEventHandler(ngx_event_t *ev);

    /*
     * Async connector callback to provide results of async User Info request
     */
    static void handleUserInfoResponse(const ::boost::shared_ptr< ::std::exception >& err,
            const ::boost::shared_ptr< ::ezbake::nginx::connector::NginxConnectorInterface::AuthenticationData::UserInfo>& authData,
            const RequestContext& reqContext);

    /*
     * Async connector callback to provide results for async User Json request
     */
    static void handleUserJsonResponse(const ::boost::shared_ptr< ::std::exception >& err,
            const ::boost::shared_ptr< ::ezbake::nginx::connector::NginxConnectorInterface::AuthenticationData::UserJson>& authData,
            const RequestContext& reqContext);

private:
    static std::string getFormattedName(X509_NAME *name);
    static void getCertNames(X509* cert, ::ezbake::base::thrift::X509Info& x509Info);

    static ngx_int_t verifyWithEzSecurity(ngx_http_eznginx_auth_ctx_t *ctx);
    static ngx_int_t startVerfication(ngx_http_eznginx_auth_ctx_t *ctx, const ::ezbake::base::thrift::X509Info& x509);

    static void updateEzbUserInfoForContext(ngx_http_eznginx_auth_ctx_t *ctx,
            const ::ezbake::nginx::connector::NginxConnectorInterface::AuthenticationData::UserInfo& authData);
    static void updateEzbUserJsonForContext(ngx_http_eznginx_auth_ctx_t *ctx,
            const ::ezbake::nginx::connector::NginxConnectorInterface::AuthenticationData::UserJson& authData);

    static void resetPendingRequestContexts(const RequestContext& initiatingContext);
    static void removeRequestContext(const RequestContext& context);
    static void addRequestContext(const RequestContext& context);
    static bool isRequestContextPending(const RequestContext& context) {
        std::lock_guard<std::recursive_mutex> lock(_mLock);
        return (_pendingRequestContexts.end() != _pendingRequestContexts.find(context));
    }

public:
    virtual ~NginxAuthModule() {}

private:
    NginxAuthModule() {}

private:
    static ::boost::shared_ptr< connector::NginxAsyncConnector> _connector;
    static ::boost::shared_ptr< ::ezbake::configuration::EZConfiguration> _configuartion;
    static ::std::recursive_mutex _mLock;
    static ::std::set<RequestContext> _pendingRequestContexts;
};

}} // ezbake::nginx

#endif /* EZBAKE_NGINX_MODULE_NGINXAUTHMODULE_H_ */
