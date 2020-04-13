package warp

//CMCredential is the basic Credential Management Credential type that
//is inherited by PublicKeyCredential
type CMCredential struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

//PublicKeyCredential inherits from Credential and contains the attributes that
//are returned to the caller when a new credential is created, or a new
//assertion is requested.
type PublicKeyCredential struct {
	CMCredential
	RawID      []byte                                `json:"rawId"`
	Extensions AuthenticationExtensionsClientOutputs `json:"extensions,omitempty"`
}

//AttestationPublicKeyCredential is the PublicKeyCredential returned from a call
//to navigator.credentials.create(), with an AuthenticatorAttestationResponse
type AttestationPublicKeyCredential struct {
	PublicKeyCredential
	Response AuthenticatorAttestationResponse `json:"response"`
}

//AssertionPublicKeyCredential is the PublicKeyCredential returned from a call
//to navigator.credentials.get(), with an AuthenticatorAssertionResponse
type AssertionPublicKeyCredential struct {
	PublicKeyCredential
	Response AuthenticatorAssertionResponse `json:"response"`
}

//CredentialCreationOptions specifies the parameters to create a credential
type CredentialCreationOptions struct {
	PublicKey PublicKeyCredentialCreationOptions `json:"publicKey"`
}

//CredentialRequestOptions specifies the parameters to retrieve a credential
type CredentialRequestOptions struct {
	PublicKey PublicKeyCredentialRequestOptions `json:"publicKey"`
}
