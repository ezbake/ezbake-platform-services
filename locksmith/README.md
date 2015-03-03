# Locksmith

Locksmith was created to solve EzBake’s encryption problem. The distributed nature of EzBake complicates key
distribution. As additional instances of a service are spun up, each instance needs to have access to the
application’s keys. Since we don’t know the physical location of each instance ahead of time, we needed a secure way to
provide access to those keys at runtime.

Locksmith is a thrift service that generates encryption keys, stores them, and provides restricted access to those keys.

## Key Access Control

Access to symmetric keys and private keys is restricted to the owner of the key, and other applications the key is
shared with.

Locksmith uses EzSecurityTokens to identify users of the service. The security Id of the application the token was
issued to is considered to be the identity of the caller. When a key is initially generated, the identity of the owner
and any applications the key is shared with is established. Keys are only returned to the caller if the security Id
from the token is either an owner, or the key is shared with that app.

For asymmetric keys, access to public keys is unrestricted.

## Dependencies

* MongoDB

## Deployment

Locksmith is relatively easy to deploy, it requires no custom configuration files. There is a sample manifest file at
service/deployment/locksmith.yml.
