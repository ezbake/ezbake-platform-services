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



#ifndef EZBAKE_NGINX_CONNECTOR_NGINXASYNCCONNECTOR_H_
#define EZBAKE_NGINX_CONNECTOR_NGINXASYNCCONNECTOR_H_

#include <ezbake/nginx/connector/NginxConnectorInterface.h>
#include <ezbake/security/client/AsyncClient.h>

namespace ezbake { namespace nginx { namespace connector {

class NginxAsyncConnector : public NginxConnectorInterface {
public:
    typedef ::boost::function<void(const ::boost::shared_ptr< ::std::exception >&,
                                   const ::boost::shared_ptr< AuthenticationData::UserInfo>& )> AuthenticateUserInfoCallback;
    typedef ::boost::function<void(const ::boost::shared_ptr< ::std::exception >&,
                                   const ::boost::shared_ptr< AuthenticationData::UserJson>& )> AuthenticateUserJsonCallback;


public:
    /*
     * Constructor/Destructor
     */
    NginxAsyncConnector(const ::ezbake::configuration::EZConfiguration& config, const std::string& configNamespace="");
    virtual ~NginxAsyncConnector() {}

    /*
     * Authenticates the user and retrieves the User Info
     *
     * @param x509Info   x509 info of user to authenticate
     *
     * @throws exception if user could not be authenticated
     */
    ::boost::optional<AuthenticationData::UserInfo> authenticateUserInfo(const AuthenticateUserInfoCallback& cb,
            const ::ezbake::base::thrift::X509Info& x509Info);

    /*
     * Authenticates the user and retrieves the User Json
     *
     * @param x509Info   x509 info of user to authenticate
     *
     * @throws exception if user could not be authenticated
     */
    ::boost::optional<AuthenticationData::UserJson> authenticateUserJson(const AuthenticateUserJsonCallback& cb,
            const ::ezbake::base::thrift::X509Info& x509Info);

    /**
     * Initializes the connector
     *
     * @throws exceptions on initialization errors
     */
    virtual void initialize();

protected:
    void handleProxyTokenCallback(const boost::shared_ptr< ::std::exception >& err,
            const ::boost::shared_ptr< ::ezbake::security::thrift::ProxyTokenResponse>& dn,
            const AuthenticateUserInfoCallback& cb, const ::ezbake::base::thrift::X509Info& x509Info);

    void handleUserJsonCallback(const boost::shared_ptr< ::std::exception >& err,
            const ::boost::shared_ptr< ::ezbake::base::thrift::EzSecurityTokenJson>& json,
            const AuthenticateUserJsonCallback& cb, const ::ezbake::base::thrift::X509Info& x509Info);

private:
    ::std::string _configNamespace;

    ::ezbake::configuration::EZConfiguration _configuration;
    ::boost::shared_ptr< ::ezbake::configuration::helpers::ApplicationConfiguration> _appConfig;
    ::boost::shared_ptr< ::ezbake::security::client::AsyncClient> _securityClient;
};

}}} //ezbake::nginx::connector

#endif /* EZBAKE_NGINX_CONNECTOR_NGINXASYNCCONNECTOR_H_ */
