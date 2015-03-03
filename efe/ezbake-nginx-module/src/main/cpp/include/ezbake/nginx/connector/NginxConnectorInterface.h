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



#ifndef EZBAKE_NGINX_CONNETOR__NGINXCONNECTORINTERFACE_H_
#define EZBAKE_NGINX_CONNETOR__NGINXCONNECTORINTERFACE_H_

#include <string>
#include <iomanip>
#include <sstream>
#include <iomanip>
#include <ezbake/security/core/CommonUtils.h>
#include <ezbake/security/core/EzSecurityTokenUtils.h>

namespace ezbake { namespace nginx { namespace connector {

class NginxConnectorInterface {
public:
    class AuthenticationData {
    public:
        class UserInfo {
        public:
            ::std::string userDN;
            ::std::string proxyTokenString;
            ::std::string proxyTokenSignature;
        };

        class UserJson {
        public:
            ::std::string jsonString;
            ::std::string jsonSignature;
        };
    };

public://constants
    static const int TIME_STR_WIDTH = 21;

public:
    virtual ~NginxConnectorInterface(){}

    /**
     * Generate a security token request for the specified DN
     *
     * @param formattedDN   formatted DN of the request
     * @param securityId    security ID of the requester
     * @param securityConfig    security configuration for signing the request
     *
     * @return the generated token
     */
    static ::ezbake::base::thrift::TokenRequest generateTokenRequest(const ::ezbake::base::thrift::X509Info& x509Info,
            const ::std::string& securityId, const ::ezbake::configuration::helpers::SecurityConfiguration& securityConfig);

    /**
     * Generate a security proxy token request for the specified DN
     *
     * @param formattedDN       formatted DN of the request
     * @param securityID        security ID of the requester
     * @param securityConfig    security configuration for signing the request
     */
    static ::ezbake::security::thrift::ProxyTokenRequest generateProxyTokenRequest(const ::ezbake::base::thrift::X509Info& x509Info,
            const ::std::string& securityId, const ::ezbake::configuration::helpers::SecurityConfiguration& securityConfig);

    /**
     * Return the string representation of the specified time
     */
    static ::std::string convertTimeMillisToStr(int64_t time);

    /**
     * Determine if the proxy token response has expired
     */
    static bool hasAuthenticationExpired(const ::ezbake::security::thrift::ProxyTokenResponse& token);
    static bool hasAuthenticationExpired(const ::ezbake::base::thrift::ProxyUserToken& token);

    /*
     * Initializes the connector
     *
     * @throws exceptions on initialization errors
     */
    virtual void initialize() = 0;

};

}}} //ezbake::nginx::connector

#endif /* EZBAKE_NGINX_CONNETOR__NGINXCONNECTORINTERFACE_H_ */
