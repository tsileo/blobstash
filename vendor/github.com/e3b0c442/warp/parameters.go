package warp

//PublicKeyCredentialParameters is used to supply additional parameters when
//creating a new credential.
type PublicKeyCredentialParameters struct {
	Type PublicKeyCredentialType `json:"type"`
	Alg  COSEAlgorithmIdentifier `json:"alg"`
}
