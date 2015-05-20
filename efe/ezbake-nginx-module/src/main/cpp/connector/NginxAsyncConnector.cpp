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


#include <ezbake/nginx/connector/NginxAsyncConnector.h>
#include "EzSecurityServices_types.h"
#include "EzReverseProxy_constants.h"

namespace {
log4cxx::LoggerPtr const LOG = log4cxx::Logger::getLogger("ezbake.nginx.connector.NginxAsyncConnector");
}

namespace ezbake { namespace nginx { namespace connector {

using ::ezbake::base::thrift::EzSecurityPrincipal;
using ::ezbake::base::thrift::EzSecurityTokenJson;
using ::ezbake::security::thrift::ProxyTokenRequest;
using ::ezbake::security::thrift::ProxyTokenResponse;
using ::ezbake::base::thrift::TokenRequest;
using ::ezbake::security::core::EzSecurityTokenUtils;
using ::ezbake::configuration::helpers::ApplicationConfiguration;


NginxAsyncConnector::NginxAsyncConnector(const ::ezbake::configuration::EZConfiguration& config, const std::string& configNamespace)
    : _configNamespace(configNamespace), _configuration(config) {}


::boost::optional<NginxConnectorInterface::AuthenticationData::UserInfo>
NginxAsyncConnector::authenticateUserInfo(const AuthenticateUserInfoCallback& cb, const ::ezbake::base::thrift::X509Info& x509Info) {
    ::boost::shared_ptr<AuthenticationData::UserInfo> authData;
    ProxyTokenRequest proxyRequest = generateProxyTokenRequest(x509Info,
            _appConfig->getSecurityID(), _securityClient->getSecurityConfig());

    LOG4CXX_INFO(LOG, "Requesting ProxyToken for user {" << x509Info.subject << "}.");

    ::boost::optional<ProxyTokenResponse> proxyTokenOpt =
            _securityClient->fetchProxyToken(::boost::bind(&NginxAsyncConnector::handleProxyTokenCallback,
                                                           this, _1, _2, cb, x509Info), proxyRequest);

    if (proxyTokenOpt) {
        LOG4CXX_INFO(LOG, "got ProxyTokenResponse user {" << x509Info.subject << "} from cache. "
                "Credentials expire at " << EzSecurityTokenUtils::getProxyTokenResponseExpiration(*proxyTokenOpt));

        authData = ::boost::make_shared<AuthenticationData::UserInfo>();
        authData->userDN = x509Info.subject;
        authData->proxyTokenString = proxyTokenOpt->token;
        authData->proxyTokenSignature = proxyTokenOpt->signature;
        return ::boost::optional<NginxConnectorInterface::AuthenticationData::UserInfo>(*authData);
    }

    LOG4CXX_DEBUG(LOG, "Could not retrieve ProxyToken from cache. Async dispatch pending");
    return ::boost::none;
}


::boost::optional<NginxConnectorInterface::AuthenticationData::UserJson>
NginxAsyncConnector::authenticateUserJson(const AuthenticateUserJsonCallback& cb, const ::ezbake::base::thrift::X509Info& x509Info) {
    ::boost::shared_ptr<AuthenticationData::UserJson> authData;
    TokenRequest tokenRequest = generateTokenRequest(x509Info, _appConfig->getSecurityID(),
            _securityClient->getSecurityConfig());

    LOG4CXX_INFO(LOG, "Requesting UserInfoJson for user {" << x509Info.subject << "}.");

    ::boost::optional<EzSecurityTokenJson> jsonOpt =
            _securityClient->fetchUserJson(::boost::bind(&NginxAsyncConnector::handleUserJsonCallback,
                                                         this, _1, _2, cb, x509Info), tokenRequest);

    if (jsonOpt) {
        LOG4CXX_INFO(LOG, "got EzSecurityTokenJson user {" << x509Info.subject << "} from cache. "
                "Credentials expire at " << EzSecurityTokenUtils::getEzSecurityTokenJsonExpiration(*jsonOpt));

        authData = ::boost::make_shared<AuthenticationData::UserJson>();
        authData->jsonString = jsonOpt->json;
        authData->jsonSignature = jsonOpt->signature;
        return ::boost::optional<NginxConnectorInterface::AuthenticationData::UserJson>(*authData);
    }

    LOG4CXX_DEBUG(LOG, "Could not retrieve UserInfoJson from cache. Async dispatch pending");
    return ::boost::none;
}


void NginxAsyncConnector::initialize() {
    if (_securityClient) {
        LOG4CXX_WARN(LOG, "nginx connector already initialized. Reinitializing");
        _securityClient.reset();
    }

    LOG4CXX_INFO(LOG, "initializing nginx connector");

    try {
        _appConfig = ApplicationConfiguration::fromConfiguration(_configuration, _configNamespace);
        _securityClient = ::boost::make_shared< ::ezbake::security::client::AsyncClient>(_configuration,
                _configNamespace, "common_services");
    } catch (const std::exception &ex) {
        LOG4CXX_ERROR(LOG, "error in initializing nginx connector: " + boost::diagnostic_information(ex));
        throw;
    }
}


void NginxAsyncConnector::handleProxyTokenCallback(const ::boost::shared_ptr< ::std::exception >& err,
        const ::boost::shared_ptr<ProxyTokenResponse>& proxyTokenRsp, const AuthenticateUserInfoCallback& cb,
        const ::ezbake::base::thrift::X509Info& x509Info) {
    if (err) {
        LOG4CXX_ERROR(LOG, "Authentication error. Couldn't get ProxyTokenResponse data: " << err->what());
        cb(err, ::boost::shared_ptr<AuthenticationData::UserInfo>());
        return;
    }

    try {
        ::boost::shared_ptr<NginxConnectorInterface::AuthenticationData::UserInfo> authData =
                ::boost::make_shared<NginxConnectorInterface::AuthenticationData::UserInfo>();

        authData->userDN = x509Info.subject;
        authData->proxyTokenString = proxyTokenRsp->token;
        authData->proxyTokenSignature = proxyTokenRsp->signature;

        LOG4CXX_INFO(LOG, "received async call response for User Info {" << x509Info.subject
                << "} from security service. Invoking nginx auth handler");

        cb(::boost::shared_ptr< ::std::exception >(), authData);
        return;
    } catch (const ::std::exception &ex) {
        LOG4CXX_ERROR(LOG, "Exception in handling ProxyTokenCallback: " << ex.what());
        cb(::boost::make_shared< ::std::runtime_error>(ex.what()),
                ::boost::shared_ptr<AuthenticationData::UserInfo>());
    }
}


void NginxAsyncConnector::handleUserJsonCallback(const ::boost::shared_ptr< ::std::exception >& err,
        const ::boost::shared_ptr<EzSecurityTokenJson>& json, const AuthenticateUserJsonCallback& cb,
        const ::ezbake::base::thrift::X509Info& x509Info) {
    if (err) {
        LOG4CXX_ERROR(LOG, "Authentication error. Couldn't get UserJson: " << err->what());
        cb(err, ::boost::shared_ptr<AuthenticationData::UserJson>());
        return;
    }

    try {
        ::boost::shared_ptr<NginxConnectorInterface::AuthenticationData::UserJson> authData =
                ::boost::make_shared<NginxConnectorInterface::AuthenticationData::UserJson>();

        authData->jsonString = json->json;
        authData->jsonSignature = json->signature;

        LOG4CXX_INFO(LOG, "received async call response for User Json {" << x509Info.subject
                << "} from security service. Invoking nginx auth handler");

        cb(::boost::shared_ptr< ::std::exception >(), authData);
        return;
    } catch (const std::exception &ex) {
        LOG4CXX_ERROR(LOG, "Exception in handling UserJsonCallback: " << ex.what());
        cb(::boost::make_shared< ::std::runtime_error>(ex.what()),
                ::boost::shared_ptr<AuthenticationData::UserJson>());
    }
}


}}} //ezbake::nginx::connector
