package warp

//PublicKeyCredentialRequestOptions supplies get() with the data it needs to
//generate an assertion.
type PublicKeyCredentialRequestOptions struct {
	Challenge        []byte                               `json:"challenge"`
	Timeout          uint                                 `json:"timeout,omitempty"`
	RPID             string                               `json:"rpId,omitempty"`
	AllowCredentials []PublicKeyCredentialDescriptor      `json:"allowCredentials,omitempty"`
	UserVerification UserVerificationRequirement          `json:"userVerification,omitempty"`
	Extensions       AuthenticationExtensionsClientInputs `json:"extensions,omitempty"`
}
