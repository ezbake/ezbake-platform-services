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


#include <ezbake/nginx/connector/NginxSyncConnector.h>
#include "EzSecurityServices_types.h"

namespace {
log4cxx::LoggerPtr const LOG = log4cxx::Logger::getLogger("ezbake.nginx.connector.NginxSyncConnector");
}

namespace ezbake { namespace nginx { namespace connector {

using ::ezbake::base::thrift::EzSecurityPrincipal;
using ::ezbake::base::thrift::EzSecurityTokenJson;
using ::ezbake::base::thrift::TokenRequest;
using ::ezbake::security::thrift::ProxyTokenRequest;
using ::ezbake::security::thrift::ProxyTokenResponse;
using ::ezbake::configuration::helpers::ApplicationConfiguration;


NginxSyncConnector::NginxSyncConnector(const ::ezbake::configuration::EZConfiguration& config, const ::std::string& configNamespace)
    : _configNamespace(configNamespace), _configuration(config) {}


NginxConnectorInterface::AuthenticationData::UserInfo NginxSyncConnector::authenticateUserInfo(const ::ezbake::base::thrift::X509Info& x509Info) {
    NginxConnectorInterface::AuthenticationData::UserInfo retVal;

    if (!_securityClient) {
        initialize();
    }

    try {
        ProxyTokenRequest proxyRequest = generateProxyTokenRequest(x509Info,
                _appConfig->getSecurityID(), _securityClient->getSecurityConfig());

        LOG4CXX_INFO(LOG, "attempting to authenticate & retrieve user info for {" << x509Info.subject << "} with security service");

        ProxyTokenResponse proxyResponse = _securityClient->fetchProxyToken(proxyRequest);

        if (hasAuthenticationExpired(proxyResponse)) {
            LOG4CXX_ERROR(LOG, "Authentication error. Received expired Principal data from security service.");
            ::ezbake::base::thrift::EzSecurityTokenException ex;
            ex.message = "Principal received from security service has expired";
            BOOST_THROW_EXCEPTION(ex);
        }

        retVal.userDN = x509Info.subject;
        retVal.proxyTokenString = proxyResponse.token;
        retVal.proxyTokenSignature = proxyResponse.signature;

    } catch(const ::ezbake::base::thrift::EzSecurityTokenException &ex) {
        std::string message = "Internal Error. EzSecurityTokenException: " + ex.message;
        LOG4CXX_ERROR(LOG, message);
        BOOST_THROW_EXCEPTION(::apache::thrift::TException(message));
    } catch(const ::ezbake::security::thrift::UserNotFoundException &ex) {
        std::string message = "User Not Found (or external user database down): " + ex.message;
        LOG4CXX_ERROR(LOG, message);
        BOOST_THROW_EXCEPTION(::apache::thrift::TException(message));
    } catch(const ::apache::thrift::TException &ex) {
        std::string message = "Internal Error. Generic Thrift Error:  ";
        LOG4CXX_ERROR(LOG, message + boost::diagnostic_information(ex));
        message += ex.what();
        BOOST_THROW_EXCEPTION(::apache::thrift::TException(message));
    } catch(const ::std::exception &ex) {
        std::string message = "Internal Error. Generic STD Exception: ";
        LOG4CXX_ERROR(LOG, message + boost::diagnostic_information(ex));
        message += ex.what();
        BOOST_THROW_EXCEPTION(std::runtime_error(message));
    }

    LOG4CXX_INFO(LOG, "authenticated user {" << x509Info.subject << "} with security service");
    return retVal;
}


NginxConnectorInterface::AuthenticationData::UserJson NginxSyncConnector::authenticateUserJson(const ::ezbake::base::thrift::X509Info& x509Info) {
    NginxConnectorInterface::AuthenticationData::UserJson retVal;

    if (!_securityClient) {
        initialize();
    }

    try {
        TokenRequest jsonRequest = generateTokenRequest(x509Info, _appConfig->getSecurityID(),
                _securityClient->getSecurityConfig());

        LOG4CXX_INFO(LOG, "attempting to & retrieve user json for  {" << x509Info.subject << "} with security service");

        EzSecurityTokenJson jsonResponse = _securityClient->fetchUserJson(jsonRequest);
        retVal.jsonString= jsonResponse.json;
        retVal.jsonSignature = jsonResponse.signature;

    } catch(const ::ezbake::base::thrift::EzSecurityTokenException &ex) {
        std::string message = "Internal Error. EzSecurityTokenException: " + ex.message;
        LOG4CXX_ERROR(LOG, message);
        BOOST_THROW_EXCEPTION(::apache::thrift::TException(message));
    } catch(const ::apache::thrift::TException &ex) {
        std::string message = "Internal Error. Generic Thrift Error:  ";
        LOG4CXX_ERROR(LOG, message + boost::diagnostic_information(ex));
        message += ex.what();
        BOOST_THROW_EXCEPTION(::apache::thrift::TException(message));
    } catch(const ::std::exception &ex) {
        std::string message = "Internal Error. Generic STD Exception: ";
        LOG4CXX_ERROR(LOG, message + boost::diagnostic_information(ex));
        message += ex.what();
        BOOST_THROW_EXCEPTION(std::runtime_error(message));
    }

    LOG4CXX_INFO(LOG, "authenticated user {" << x509Info.subject << "} with security service");
    return retVal;
}


void NginxSyncConnector::initialize() {
    if (_securityClient) {
        LOG4CXX_WARN(LOG, "nginx connector already initialized. Reinitializing");
        _securityClient.reset();
    }

    LOG4CXX_INFO(LOG, "initializing nginx connector");

    try {
        _appConfig = ApplicationConfiguration::fromConfiguration(_configuration, _configNamespace);
        _securityClient = ::boost::make_shared< ::ezbake::security::client::SyncClient>(_configuration, _configNamespace);
    } catch (const std::exception &ex) {
        LOG4CXX_ERROR(LOG, "error in initializing nginx connector: " + boost::diagnostic_information(ex));
        throw;
    }
}

}}} //ezbake::nginx::connector
