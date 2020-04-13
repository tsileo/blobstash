# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.0] - 2020-02-02
### Added
- Extension implementations for `txAuthSimple` and `txAuthGeneric`.

## [0.5.0] - 2020-01-26
### Added
- `RegistrationValidator` and `AuthenticationValidator` function type for passing additional validations into the `Finish...` functions. These functions take pointerl to the ceremonies' respective options and credential structs as arguments, and as such can modify those values as needed. If the functions return an error, the ceremony ends in error. The `Finish...` functions will continue to have all validations required by the specification implemented.
- Added `ValidateAppID` function conforming to `AuthenticationValidator` which replaces `VerifyAppID` and `EffectiveRPID`

### Changed
- `FinishRegistration` and `FinishAuthentication` now accept zero or more `RegistrationValidator` or `AuthenticationValidator` functions respectively as variable arguments. Any provided validators are run BEFORE any of the required validation checks, and end the ceremony if they return an error. Note that this is NOT a breaking change as zero of these arguments may be provided.

### Removed
- **[BREAKING]** Removed the `ExtensionValidator` type in favor of the new general `RegistrationValidator` and `AuthenticationValidator` types.
- **[BREAKING]** Removed the `EffectiveRPID` function in favor of updating the RPID via a validator function
- **[BREAKING]** Removed the `VerifyAppID` function in favor of `ValidateAppID` (noted above)

## [0.4.0] - 2020-01-25
### Added
- Attestation verification for the _packed_ format has been added, continuing the previous guidance for trust chain validation. ECDAA attestation type is not supported due to lack of ECDAA support in Go standard library.

## [0.3.0] - 2020-01-21
### Added
- Attestation verification for the _fido-u2f_ format has been added. The attestation type and trust path are __not__ validated, only the signature against the provided certificate. It is up to the implementor to verify the trust chain using the `*AttestationObject` returned from `FinishRegistration`.
### Changed
- Minor changes to fix static analysis deficiencies discovered with `staticcheck`
- Added contributing instructions
- Added contributors list
- Documentation updates as needed in order to CII self-certify
- Updated CI pipeline with additional checks

## [0.2.0] - 2020-01-19
### Added
- `UnmarshalBinary` and `MarshalBinary` methods on `AttestationObject` and `AuthenticatorData`, implementing the `BinaryMarshaler` and `BinaryUnmarshaler` interfaces
- `Encode` method on `AuthenticatorData` to facilitate encoding `AttestationObject` for storage
### Changed
- **[BREAKING]** `FinishRegistration` now returns `(*AttestationObject, error)` instead of `(string, []byte, error)`, to allow the implementor to choose how much or little of the authenticator data to save.
- **[BREAKING]** `FinishAuthentication` now returns `(*AuthenticatorData, error)` instead of `(uint, error)`, to allow the implementor full access to the authenticator data for other uses
- **[BREAKING]** AttestationObject now holds the parsed AuthenticatorData instead of the raw bytes
- **[BREAKING]** Rename methods on `RelyingParty`, `User`, and `Credential` interfaces to reduce the risk of conflicts with lower-order data members
- **[BREAKING]** Change `EntityID()` (formerly `ID()`) method on `Credential` interface to return `[]byte` instead of `string`
- **[BREAKING]** Change `CredFinder` function type to accept argument of type `[]byte` instead of `string`
- **[BREAKING]** `AttestedCredentialData` `CredentialPublicKey` member is now the raw `cbor.RawMessage` instead of the parsed `COSEKey`
- Changed `verifyAttestationStatement` to take the `AttestationObject` instead of its separated components.
- Updated [github.com/fxamacker/cbor](https://github.com/fxamacker/cbor) to version 1.5.0 and changed encoding options on all calls to the new convenience functions
- Updated the demo app to reflect breaking changes


## [0.1.0] - 2020-01-14
### Added
- Initial implementation
