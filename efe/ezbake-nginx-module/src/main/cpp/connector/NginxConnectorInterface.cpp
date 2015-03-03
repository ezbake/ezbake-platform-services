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



#include <ezbake/nginx/connector/NginxConnectorInterface.h>
#include <ezbake/security/core/CommonUtils.h>
#include <ezbake/security/client/CommonClient.h>


#if defined _INTTYPES_H && !defined PRId64
# pragma message "<inttypes.h> already included but PRId64 not defined. Redefining"
# if __WORDSIZE == 64
#   define PRId64   "ld"
# else
#   define PRId64   "lld"
# endif
#endif

#pragma GCC diagnostic ignored "-Wconversion"
#define PICOJSON_USE_INT64
#include "contrib/picojson/picojson.h"
#pragma GCC diagnostic warning "-Wconversion"




namespace ezbake { namespace nginx { namespace connector {

using ::ezbake::security::core::EzSecurityTokenUtils;
using ::ezbake::base::thrift::ProxyUserToken;
using ::ezbake::security::thrift::ProxyTokenResponse;
using ::ezbake::configuration::helpers::SecurityConfiguration;


::ezbake::base::thrift::TokenRequest NginxConnectorInterface::generateTokenRequest(const ::ezbake::base::thrift::X509Info& x509Info,
        const ::std::string& securityId, const SecurityConfiguration& securityConfig) {

    ::ezbake::base::thrift::EzSecurityPrincipal principal;
    principal.principal = x509Info.subject;
    principal.validity.issuer = securityId;
    principal.validity.issuedTo = "EzSecurity";
    principal.validity.notAfter = ::ezbake::security::core::CommonUtils::currentTimeMillis() +
                                  ::ezbake::security::client::CommonClient::PRINCIPAL_EXPIRY;
    principal.validity.signature = "";

    ::ezbake::base::thrift::TokenRequest request;
    request.__set_securityId(securityId);
    request.__set_timestamp(::ezbake::security::core::CommonUtils::currentTimeMillis());
    request.__set_principal(principal);
    request.__set_type(::ezbake::base::thrift::TokenType::USER);
    request.__set_caveats(principal.validity);

    //update signature in TokenRequest.ValidityCaveats
    request.caveats.signature = EzSecurityTokenUtils::tokenRequestSignature(request, securityConfig);

    return request;
}


::ezbake::security::thrift::ProxyTokenRequest NginxConnectorInterface::generateProxyTokenRequest(const ::ezbake::base::thrift::X509Info& x509Info,
        const ::std::string& securityId, const SecurityConfiguration& securityConfig) {

    ::ezbake::base::thrift::ValidityCaveats validityCaveats;
    validityCaveats.__set_issuer(securityId);
    validityCaveats.__set_issuedTo("");
    validityCaveats.__set_notAfter(::ezbake::security::core::CommonUtils::currentTimeMillis() +
                                   ::ezbake::security::client::CommonClient::PRINCIPAL_EXPIRY);
    validityCaveats.__set_signature("");

    ::ezbake::security::thrift::ProxyTokenRequest request;
    request.__set_x509(x509Info);
    request.__set_validity(validityCaveats);

    //update signature
    request.validity.signature = EzSecurityTokenUtils::proxyTokenRequestSignature(request, securityConfig);

    return request;
}


::std::string NginxConnectorInterface::convertTimeMillisToStr(int64_t time) {
    ::std::ostringstream ss;
    ss << ::std::setfill('0') << ::std::setw(TIME_STR_WIDTH) << time;
    return ss.str();
}


bool NginxConnectorInterface::hasAuthenticationExpired(const ProxyTokenResponse& proxyToken) {
    picojson::value pj;
    ::std::string err = picojson::parse(pj, proxyToken.token);

    if (!err.empty()) {
        BOOST_THROW_EXCEPTION(::std::runtime_error("Exception in parsing proxyToken.token JSON: " +
                err + "\nJSON contents:\n" + proxyToken.token));
    }

    picojson::object token = pj.get<picojson::object>();
    int64_t expiryTime = token["notAfter"].get<int64_t>();

    return (expiryTime < ::ezbake::security::core::CommonUtils::currentTimeMillis());
}


bool NginxConnectorInterface::hasAuthenticationExpired(const ProxyUserToken& token) {
    return (token.notAfter < ::ezbake::security::core::CommonUtils::currentTimeMillis());
}


}}} // namespace ezbake::nginx::connector
