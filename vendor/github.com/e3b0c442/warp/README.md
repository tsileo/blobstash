# warp - *W*eb*A*uthn *R*elying *P*arty

[![Build Status](https://travis-ci.org/e3b0c442/warp.svg?branch=master)](https://travis-ci.org/e3b0c442/warp)
[![GoDoc](https://godoc.org/github.com/e3b0c442/warp?status.svg)](https://godoc.org/github.com/e3b0c442/warp)
[![Go Report Card](https://goreportcard.com/badge/github.com/e3b0c442/warp)](https://goreportcard.com/report/github.com/e3b0c442/warp)
[![codecov](https://codecov.io/gh/e3b0c442/warp/branch/master/graph/badge.svg)](https://codecov.io/gh/e3b0c442/warp)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/3625/badge)](https://bestpractices.coreinfrastructure.org/projects/3625)
[![Release](https://img.shields.io/github/release/e3b0c442/warp.svg?style=flat-square)](https://github.com/e3b0c442/warp/releases)
![GitHub](https://img.shields.io/github/license/e3b0c442/warp)

_warp_ is a WebAuthn Relying Party implementation which is intended to be 100% compliant with the [W3C WebAuthn Level 1](https://www.w3.org/TR/webauthn-1/) standard while being HTTP implementation agnostic. It is completely standalone; simply provide your own HTTP server, backend storage and
session storage.

_Requires Go 1.13+_

**This library is still pre-v1, and API stability is not guaranteed. The library will adhere to SemVer and Go backward campatibilty promises.**

## Contents
- [Installation](#installation)
- [Quick start](#quick-start)
  - [Starting registration](#starting-registration)
  - [Finishing registration](#finishing-registration)
  - [Starting authentication](#starting-authentication)
  - [Finishing authentication](#finishing-authentication)
- [Design goals](#design-goals)
- [Specification coverage](#specification-coverage)
- [High level API](#high-level-api)
  - [Interfaces](#interfaces)
    - [`RelyingParty`](#relyingparty)
    - [`User`](#user)
    - [`Credential`](#credential)
  - [Helper functions](#helper-functions)
    - [`UserFinder`](#userfinder)
    - [`CredentialFinder`](#credentialfinder)
  - [Registration](#registration)
    - [`StartRegistration`](#startregistration)
    - [`FinishRegistration`](#finishregistration)
  - [Authentication](#authentication)
    - [`StartAuthentication`](#startauthentication)
    - [`FinishAuthentication`](#finishauthentication)
- [Contributing](#contributing)
- [Security vulnerabilities](#security-vulnerabilities)
- [License](#license)
## Installation

1. You must have Go _1.13+_ installed.
2. Use the `go get` command to bring the library into your workspace:
```bash
$ go get github.com/e3b0c442/warp
```
3. Import into your code:
```go
import "github.com/e3b0c442/warp"
```

## Quick start

By design, _warp_ is not batteries included. Your application must have the following to use _warp_:

1. The ability to marshal and unmarshal JSON,
2. A session or other backend store to store the challenge data,
3. A config or other struct which implements the `RelyingParty` interface,
4. A user store which implements the `User` interface, and
5. A credential store which implements the `Credential` interface.

### Starting registration

The user must be known. This example assumes the session store can infer the user from the request.

```go
import (
    "encoding/json"
    "net/http"
    
    "github.com/e3b0c442/warp"
)

func StartRegistration(w http.ResponseWriter, r *http.Request) {
    //get the user data from a session cookie or similar
    session, err := SessionStore.Get(r)
    if err != nil {
        return http.Error(w, "Bad session data", http.StatusBadRequest)
    }

    //generate a challenge with the user data
    opts, err := warp.StartRegistration(relyingParty, session.User)
    if err != nil {
        return http.Error(w, "Unable to generate challenge", http.StatusInternalServerError)
    }
    //store the challenge for later verification
    session.Data["creation"] = opts
    http.SetCookie(w, session.Cookie)

    //send the challenge to the client
    json.NewEncoder(w).Encode(opts)
}
```

### Finishing registration

The implementation is responsible for parsing an `AttestationPublicKeyCredential` from the request.

```go
func FinishRegistration(w http.ResponseWriter, r *http.Request) {
    //get the user data and challenge from a session cookie or similar
    session, err := SessionStore.Get(r)
    if err != nil {
        return http.Error(w, "Bad session data", http.StatusBadRequest)
    }

    //get the stored challenge from the session store
    storedOpts, ok := session.Data["creation"]
    if !ok {
        return http.Error(w, "No creation data in session", http.StatusForbidden)
    }
    opts := storedOpts.(PublicKeyCredentialCreationOptions)
    
    //parse the credential sent from the client
    var cred AttestationPublicKeyCredential
    err := json.NewDecoder(r.Body).Decode(&cred)
    if err != nil {
        return http.Error(w, "Error parsing credential", http.StatusBadRequest)
    }

    //verify the credential against the stored challenge
    attestation, err := warp.FinishRegistration(relyingParty, CredentialStore.Find, &opts, &cred)
    if err != nil {
        return http.Error(w, "Credential verification failed", http.StatusUnauthorized)
    }

    //store the returned credential
    err := CredentialStore.Store(session.User, attestation)
    if err != nil {
        return http.Error(w, "Unable to store credential", http.StatusInternalServerError)
    }

    //return success to the client
    http.WriteHeader(http.StatusNoContent)
}
```

### Starting authentication

The user is only required to be provided for second-factor auth, as long as the session can correlate the authentication start and finish calls.

```go
func StartAuthentication(w http.ResponseWriter, r *http.Request) {
    //get or start the session data
    session, err := SessionStore.Get(r)
    if err != nil {
        session = SessionStore.Start(r)
    }

    opts, err := warp.StartAuthentication()
    if err != nil {
        return http.Error(w, "Unable to generate challenge", http.StatusInternalServerError)
    }

    //store the challenge for later verification
    session.Data["request"] = opts
    http.SetCookie(w, session.Cookie)

    //send the challenge to the client
    json.NewEncoder(w).Encode(opts)
}
```

### Finishing authentication

If the user is already known, have the passed `UserFinder` ignore the ID and just return the known user; otherwise pass a mechanism to look up based on the user handle.

```go
func FinishAuthentication(w http.ResponseWriter, r *http.Request) {
    //get or start the session data
    session, err := SessionStore.Get(r)
    if err != nil {
        return http.Error(w, "Bad session data", http.StatusBadRequest)
    }

    //get the stored challenge from the session store
    storedOpts, ok := session.Data["request"]
    if !ok {
        return http.Error(w, "No request data in session", http.StatusForbidden)
    }
    opts := storedOpts.(PublicKeyCredentialRequestOptions)
    
    //parse the credential sent from the client
    var cred AssertionPublicKeyCredential
    err := json.NewDecoder(r.Body).Decode(&cred)
    if err != nil {
        return http.Error(w, "Error parsing credential", http.StatusBadRequest)
    }

    //verify the credential
    authData, err := warp.FinishAuthentication(relyingParty, func(_ []byte) (warp.User, error) {
        return session.User, nil
    }, &opts, &cred)
    if err != nil {
        return http.Error(w, "Credential verification failed", http.StatusUnauthorized)
    }
    
    //update credential info such as signcount
    err := CredentialStore.Update(authData)
    if err != nil {
        //Implementation can decide whether this is a problem or not
    }

    //return success to the client
    http.WriteHeader(http.StatusNoContent)
}
```

## Design goals
_warp_ was built with the following goals in mind:
* 100% compliance with the WebAuthn Level 1 specification
* HTTP implementation agnostic. This library makes no assumptions about the structure or operation of your web application. Use your implementation's canonical method to parse the WebAuthn JSON.
* No assumptions about application data models. Implement and use the required interfaces wherever is appropriate in your implementation
* Minimal dependencies outside of the standard library, with those used chosen carefully to keep the dependency tree clean. At the time of this writing, the following external dependencies are used:
  * [fxamacker/cbor](https://github.com/fxamacker/cbor) - A fantastic CBOR implementation by Faye Amacker
* Simple package structure - just one package to import
* Structure and member naming parity with the WebAuthn spec, so that you can follow along and understand

## Specification coverage
* Key algorithms:
  * Supported: ES256, ES384, ES512, EdDSA, RS1, RS256, RS384, RS512, PS256, PS384, PS512
  * To be implemented: None plannned
* Attestation formats
  * Supported: _packed, _fido-u2f_, _none_
  * To be implemented: _tpm_, _android-key_, _android-safetynet_
* Defined extensions
  * Supported: _appid_, _txAuthSimple_, _txAuthGeneric_
  * To be implemented:  _authnSel_, _exts_, _uvi_, _loc_, _uvm_, _biometricPerfBounds_

## High level API
WebAuthn relying parties have two responsibilities: managing the _registration ceremony_, and managing the _authentication ceremony_. In order to support these ceremonies, interfaces are defined such that the methods will return the required data. 

### Interfaces

#### `RelyingParty`

```go
type RelyingParty interface {
	EntityID() string
	EntityName() string
	EntityIcon() string
	Origin() string
}
```

`RelyingParty` contains all of the non-user-specific data required to be stored
or configured by the relying party for use during the registration or
authentication ceremonies.
* `EntityID() string`: Returns the Relying Party ID, which scopes the credential. Credentials can only be used for authentication with the same entity (identified by RP ID) it was registered with. The RP ID must be equal to or a registrable domain suffix of the origin.
* `EntityName() string`: A human-palatable name for the Relying Party.
* `EntityIcon() string`: A URL which resolves an image associated with the Relying Party. May be the empty string.
* `Origin() string`: The fully qualified origin of the Relying Party.

#### `User`

```go
type User interface {
	EntityName() string
	EntityIcon() string
	EntityID() []byte
	EntityDisplayName() string
	Credentials() map[string]Credential
}
```

`User` contains all of the user-specific information which needs to be stored and provided during the registration and authentication ceremonies.
* `EntityName() string`: A human-palatable name for a user account, such as a username or email address
* `EntityIcon() string`: A URL which resolves to an image associated with the user. May be the empty string.
* `EntityID() string`: The user handle for the account. This should be an opaque byte sequence with a maximum of 64 bytes which does not contain any other identifying information about the user.
* `EntityDisplayName() string`: A human-palatable name for a user account, such as a user's full name, intended for display. 
* `Credentials() map[string]Credential` returns a map of objects which implement the `Credential` interface. The map is keyed by the base64url-encoded form of the credential ID.

#### `Credential`

```go
type Credential interface {
	Owner() User
	CredentialID() []byte
	CredentialPublicKey() []byte
	CredentialSignCount() uint
}
```

`Credential` contains the credential-specific information which needs to be stored and provided during the authentication ceremony to verify an authentication assertion.
* `Owner() User`: Returns the object implementing the `User` interface to which this credential belongs.
* `CredentialID() []byte`: The raw credential ID.
* `CredentialPublicKey() []byte`: The credential public key as returned from `FinishRegistration`. The key is encoded in the COSE key format, and may vary in size depending on the key algorithm.
* `CredentialSignCount() uint`: The stored signature counter. If the credential returns a signature counter that is less than this value, it is evidence of tampering or a duplicated credential, and the authentication ceremony will fail. If you do not wish to verify this, return `0` from this method.

### Helper functions

#### `UserFinder`

```go
type UserFinder func([]byte) (User, error)
```

`UserFinder` defines a function which takes a user handle as an argument and returns an object conforming to the User interface. If the user does not exist in the system, return `nil` and an appropriate error.

#### `CredentialFinder`

```go
type CredentialFinder func([]byte) (Credential, error)
```
`CredentialFinder` defines a function which takes a raw credential ID and returns an object conforming to the Credential interface. If the credential does not exist in the system, return `nil` and an appropriate error.

#### `RegistrationValidator`

```go
type RegistrationValidator func(opts *PublicKeyCredentialCreationOptions, cred *AttestationPublicKeyCredential) error
```
`RegistrationValidator` defines a function which takes the credential creation options and returend attestation credential, and performs any additional validation desired. Pointers to the two structs are passed so they can be modified if needed. `RegistrationValidator`s are run before any other validations in `FinishRegistration`

#### `AuthenticationValidator`

```go
type AuthenticationValidator func(opts *PublicKeyCredentialRequestOptions, cred *AssertionPublicKeyCredential) error
```
`AuthenticationValidator` defines a function which takes the credential request options and returned assertion credential, and performs any additional validation desired. Pointers to the two structs are passed so they can be modified if needed. `AuthenticationValidator`s are run before any other validations in `FinishAuthentication`


### Registration:
![Registration flow](https://www.w3.org/TR/webauthn-1/images/webauthn-registration-flow-01.svg)

#### `StartRegistration`
```go
func StartRegistration(rp RelyingParty, user User, opts ...Option) (*PublicKeyCredentialCreationOptions, error)
```

`StartRegistration` begins the registration ceremony by generating a cryptographic challenge and sending it to the client along with information about the user and Relying Party in the form of a [`PublicKeyCredentialCreationOptions`](https://www.w3.org/TR/webauthn-1/#dictionary-makecredentialoptions) object. 

The returned object or its data must be stored in the server-side session cache such that it can be reconstructed to pass to the
`FinishRegistration` function.

##### Parameters:
* `rp`: any value which implements the `RelyingParty` interface.optional if the default port for the scheme is being used.
* `user`: any value which implements the `User` interface.
* `opts`: zero or more `Option` functions to adjust the `PublicKeyCredentialCreationOptions` as needed. These do not need to be set, and likely shouldn't unless you know what you are doing. The following function generators are included:
  * `Timeout(uint)`: Sets the client timeout
  * `ExcludeCredentials([]PublicKeyCredentialDescriptors)`: Provides a list of credentials to exclude
  * `AuthenticatorSelection(AuthenticatorSelectionCriteria)`: Sets the criteria for choosing an authenticator
  * `Attestation(AttestationConveyancePreference)`: Sets the preferred attestation conveyance
  * `Extensions(...Extension)` takes zero or more `Extension` which can be used to set WebAuthn client extension inputs

##### Return values:
* A pointer to a `PublicKeyCredentialCreationOptions` struct. This value must be marshaled to JSON and returned to the client. It must also be stored in a server-side session cache in order to verify the client's subsequent response. Returns `nil` on error.
* An error if there was a problem generating the options struct, or `nil` on success.

#### `FinishRegistration`

```go
func FinishRegistration(rp RelyingParty, credFinder CredentialFinder, opts *PublicKeyCredentialCreationOptions, cred *AttestationPublicKeyCredential, vals ...RegistrationValidator) (*AttestationObject, error)
```

`FinishRegistration` completes the registration ceremony, by verifying the public key credential sent by the client against the stored creation options. If the verification is successful, a credential ID and public key are returned which must be stored. It is the responsibility of the implementor to store these and associate with the calling user.

##### Parameters:
* `rp`: An object which implements the `RelyingParty` interface
* `credFinder`: A function conforming to `CredentialFinder` which is used to check if a credential ID already exists in the system
* `opts`: A pointer to the stored `PublicKeyCredentialCreationOptions` which was previously sent to the client
* `cred`: The parsed `AttestationPublicKeyCredential` that was sent from the client in response to the server challenge
* `vals`: Additional custom validations to be performed in addition to those required by the specification

##### Return values:
* An `*AttestationObject` which contains all of the information that may need to be stored to authenticate using the credential
* An error identifying the cause of the registration failure, or `nil` on success.

### Authentication
![Authentication flow](https://www.w3.org/TR/webauthn-1/images/webauthn-authentication-flow-01.svg)

#### StartAuthentication

```go
func StartAuthentication(opts ...Option) (*PublicKeyCredentialRequestOptions, error)
```

`StartAuthentication` starts the authentication ceremony by generating a cryptographic challenge and sending it to the client along with options in the form of a [`PublicKeyCredentialRequestOptions`](https://www.w3.org/TR/webauthn-1/#assertion-options) object.

##### Parameters:
* `opts`: zero or more `Option` functions to adjust the `PublicKeyCredentialRequestOptions` as needed. These do not need to be set, and likely shouldn't unless you know what you are doing. The follownig function generators are included:
  * `Timeout(uint)`: Sets the client timeout
  * `RelyingPartyID(string)`: Adds the explicit relying party ID to the object
  * `AllowCredentials([]PublicKeyCredentialDescriptor)`: Restrict allowed credentials
  * `UserVerification(UserVerificationRequirement)`: Require user verification (PIN, biometric, etc)

##### Return values:
* A pointer to a `PublicKeyCredentialRequestOptions` struct. This value must be marshaled to JSON and returned to the client. It must also be stored in a server-side session cache in order to verify the client's subsequent response. Returns `nil` on error.
* An error if there was a problem generating the options struct, or `nil` on success.

#### FinishAuthentication

```go
func FinishAuthentication(rp RelyingParty, userFinder UserFinder, opts *PublicKeyCredentialRequestOptions, cred *AssertionPublicKeyCredential, vals ...AuthenticationValidator) (*AuthenticatorData, error)
```

`FinishAuthentication` completes the authentication ceremony by verifying the signed challenge and credential data with the stored public key for the credential. If the verification is successful, it returns a new signature counter value to be stored, and a `nil` error. Otherwise, 0 and an error describing the failure are returned. It is the responsibility of the caller to store the updated signature counter if they are choosing to verify this.

##### Parameters:
* `rp`: An object implementing the `RelyingParty` interface.
* `userFinder`: A function conforming to the `UserFinder` type which accepts a user handle as an argument and returns an object implementing the `User` interface. If the caller is not implementing the passwordless/single factor flow, the function can ignore the user handle and just return the already-known `User`.
* `opts`: A pointer to the stored `PublicKeyCredentialRequestOptions` which was previously sent to the client.
* `cred`: The parsed `AssertionPublicKeyCredential` that was sent from the client in response to the server challenge.
* `vals`: Additional custom validations to be performed in addition to those required by the specification

##### Return values:
* An `*AuthenticatorData` which contains information about the credential used to authenticate; may be used to update stored credential data such as sign count.
* An error if there was a problem verifying the user, or `nil` on success

## Contributing
Please read [CONTRIBUTING.md](https://github.com/e3b0c442/warp/blob/master/CONTRIBUTING.md) for instructions on contributing to the project.

## Contributors
Thanks to the following who have made contributions to the software or its documentation.

@virtualandy

## Security vulnerabilities
Given the impact of the authentication flow on the overall security of a project, security vulnerabilities will be taken extremely seriously.

If you discover a security vulnerability, please send an email to security@e3b0c442.io instead of opening an issue. Security vulnerabilities will be resolved within 14 days whenever feasible and the reporter and issue details will be acknowledged in the changelog once the code resolving the issue is released.

## License
Copyright (c) 2020 Nick Meyer and contributors.

This project is released under the [MIT license](https://github.com/e3b0c442/warp/blob/master/LICENSE)
