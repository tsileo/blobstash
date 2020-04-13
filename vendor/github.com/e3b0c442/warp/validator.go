package warp

//RegistrationValidator is a function that can do additional custom validations
//against the AttestationPublicKeyCredential returned by the client and the
//PublicKeyCredentialCreationOptions sent to the client. Returning a non-nil
//error ends the registration ceremony unsuccessfully.
type RegistrationValidator func(opts *PublicKeyCredentialCreationOptions, cred *AttestationPublicKeyCredential) error

//AuthenticationValidator is a function that can do additional custom
//validations against the AssertionPublicKeyCredential return by the client and
//the PublicKeyCredentialRequestOptions sent to the client. Returning a non-nil
//error ends the authentication ceremony unsuccessfully.
type AuthenticationValidator func(opts *PublicKeyCredentialRequestOptions, cred *AssertionPublicKeyCredential) error
