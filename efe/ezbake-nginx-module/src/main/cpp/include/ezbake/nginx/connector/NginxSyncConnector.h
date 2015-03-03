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



#ifndef EZBAKE_NGINX_CONNECTOR_NGINXCONNECTOR_H_
#define EZBAKE_NGINX_CONNECTOR_NGINXCONNECTOR_H_

#include <ezbake/nginx/connector/NginxConnectorInterface.h>
#include <ezbake/security/client/SyncClient.h>

namespace ezbake { namespace nginx { namespace connector {

class NginxSyncConnector : public NginxConnectorInterface {
public:
    /*
     * Constructor/Destructor
     */
    NginxSyncConnector(const ::ezbake::configuration::EZConfiguration& config,
            const ::std::string& configNamespace="");
    virtual ~NginxSyncConnector() {}


    /*
     * Authenticates the user and retrieves the User Info
     *
     * @param x509Info   x509 info of user to athenticate
     *
     * @throws exception if user could not be authenticated
     */
    AuthenticationData::UserInfo authenticateUserInfo(const ::ezbake::base::thrift::X509Info& x509Info);

    /*
     * Authenticates the user and retrieves the User Json
     *
     * @param x509Info   x509 info of user to athenticate
     *
     * @throws exception if user could not be authenticated
     */
    AuthenticationData::UserJson authenticateUserJson(const ::ezbake::base::thrift::X509Info& x509Info);

    /**
     * Initializes the connector
     *
     * @throws exceptions on initialization errors
     */
    virtual void initialize();

private:
    ::std::string _configNamespace;

    ::ezbake::configuration::EZConfiguration _configuration;
    ::boost::shared_ptr< ::ezbake::configuration::helpers::ApplicationConfiguration> _appConfig;
    ::boost::shared_ptr< ::ezbake::security::client::SyncClient> _securityClient;
};

}}} //ezbake::nginx::connector

#endif /* EZBAKE_NGINX_CONNECTOR_NGINXCONNECTOR_H_ */
