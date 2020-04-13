package warp

//AuthenticatorResponse is the is the basic authenticator response
type AuthenticatorResponse struct {
	ClientDataJSON []byte `json:"clientDataJSON"`
}

//AuthenticatorAttestationResponse represents the authenticator's response to a
//client’s request for the creation of a new public key credential.
type AuthenticatorAttestationResponse struct {
	AuthenticatorResponse
	AttestationObject []byte `json:"attestationObject"`
}

//AuthenticatorAssertionResponse represents an authenticator's response to a
//client’s request for generation of a new authentication assertion given the
//WebAuthn Relying Party's challenge and OPTIONAL list of credentials it is
//aware of.
type AuthenticatorAssertionResponse struct {
	AuthenticatorResponse
	AuthenticatorData []byte `json:"authenticatorData"`
	Signature         []byte `json:"signature"`
	UserHandle        []byte `json:"userHandle"`
}
