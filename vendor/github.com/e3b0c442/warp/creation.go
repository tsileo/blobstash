package warp

//PublicKeyCredentialCreationOptions represent options for credential creation
type PublicKeyCredentialCreationOptions struct {
	RP                     PublicKeyCredentialRPEntity          `json:"rp"`
	User                   PublicKeyCredentialUserEntity        `json:"user"`
	Challenge              []byte                               `json:"challenge"`
	PubKeyCredParams       []PublicKeyCredentialParameters      `json:"pubKeyCredParams"`
	Timeout                uint                                 `json:"timeout,omitempty"`
	ExcludeCredentials     []PublicKeyCredentialDescriptor      `json:"excludeCredentials,omitempty"`
	AuthenticatorSelection *AuthenticatorSelectionCriteria      `json:"authenticatorSelection,omitempty"`
	Attestation            AttestationConveyancePreference      `json:"attestation,omitempty"`
	Extensions             AuthenticationExtensionsClientInputs `json:"extensions,omitempty"`
}

//PublicKeyCredentialEntity describes a user account, or a WebAuthn Relying
//Party, which a public key credential is associated with or scoped to,
//respectively.
type PublicKeyCredentialEntity struct {
	Name string `json:"name"`
	Icon string `json:"icon,omitempty"`
}

//PublicKeyCredentialRPEntity is used to supply additional Relying Party
//attributes when creating a new credential.
type PublicKeyCredentialRPEntity struct {
	PublicKeyCredentialEntity
	ID string `json:"id,omitempty"`
}

//PublicKeyCredentialUserEntity is used to supply additional user account
//attributes when creating a new credential.
type PublicKeyCredentialUserEntity struct {
	PublicKeyCredentialEntity
	ID          []byte `json:"id"`
	DisplayName string `json:"displayName"`
}

//AuthenticatorSelectionCriteria may be used to specify their requirements
//regarding authenticator attributes.
type AuthenticatorSelectionCriteria struct {
	AuthenticatorAttachment AuthenticatorAttachment     `json:"authenticatorAttachment,omitempty"`
	RequireResidentKey      bool                        `json:"requireResidentKey,omitempty"`
	UserVerification        UserVerificationRequirement `json:"userVerification,omitempty"`
}

//AuthenticatorAttachment describes authenticators' attachment modalities.
type AuthenticatorAttachment string

//enum values for AuthenticatorAttachment type
const (
	AttachmentPlatform      AuthenticatorAttachment = "platform"
	AttachmentCrossPlatform AuthenticatorAttachment = "cross-platform"
)

//AttestationConveyancePreference may be used by relying parties to specify
//their preference regarding attestation conveyance during credential
//generation.
type AttestationConveyancePreference string

//enum values for AttestationConveyancePreference type
const (
	ConveyanceNone     AttestationConveyancePreference = "none"
	ConveyanceIndirect AttestationConveyancePreference = "indirect"
	ConveyanceDirect   AttestationConveyancePreference = "direct"
)
