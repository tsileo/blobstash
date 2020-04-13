//Package warp is a WebAuthn Relying Party implementation for Go that is not
//attached to net/http. Bring your own everything, parse the standards-compliant
//JSON, and pass into the appropriate functions. For full documentation visit
//https://github.com/e3b0c442/warp
package warp

//User defines functions which return data required about the authenticating
//user in order to perform WebAuthn transactions.
type User interface {
	EntityName() string
	EntityIcon() string
	EntityID() []byte
	EntityDisplayName() string
	Credentials() map[string]Credential
}

//UserFinder defines a function which takes a user handle as a parameter and
//returns an object which implements the User interface and an error
type UserFinder func([]byte) (User, error)

//Credential defines functions which return data required about the stored
//credentials
type Credential interface {
	Owner() User
	CredentialSignCount() uint
	CredentialID() []byte
	CredentialPublicKey() []byte
}

//CredentialFinder defines a function which takes a credential ID as a parameter
//and returns an object which implements the Credential interface and an error
type CredentialFinder func([]byte) (Credential, error)

//RelyingParty defines functions which return data required about the Relying
//Party in order to perform WebAuthn transactions.
type RelyingParty interface {
	EntityID() string
	EntityName() string
	EntityIcon() string
	Origin() string
}

//ChallengeLength represents the size of the generated challenge. Must be
//greater than 16.
var ChallengeLength = 32

//SupportedAttestationStatementFormats returns the list of attestation formats
//currently supported by the library
func SupportedAttestationStatementFormats() []AttestationStatementFormat {
	return []AttestationStatementFormat{
		AttestationFormatNone,
	}
}

//SupportedKeyAlgorithms returns the list of key algorithms currently supported
//by the library
func SupportedKeyAlgorithms() []COSEAlgorithmIdentifier {
	return []COSEAlgorithmIdentifier{
		AlgorithmEdDSA,
		AlgorithmES512,
		AlgorithmES384,
		AlgorithmES256,
		AlgorithmPS512,
		AlgorithmPS384,
		AlgorithmPS256,
		AlgorithmRS512,
		AlgorithmRS384,
		AlgorithmRS256,
		AlgorithmRS1,
	}
}
