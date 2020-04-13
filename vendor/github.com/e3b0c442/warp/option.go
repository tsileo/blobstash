package warp

//Option is a function that can be passed as a parameter to StartRegistration
//or StartAuthentication functions which adjusts the final options object.
//Options must typecheck for a pointer to PublicKeyCredentialCreationOptions or
//PublicKeyCredentialRequestOptions
type Option func(interface{}) error

//Timeout returns an option that adds a custom timeout to the credential
//creation options or credential request options object
func Timeout(timeout uint) Option {
	return func(in interface{}) error {
		switch opt := in.(type) {
		case *PublicKeyCredentialCreationOptions:
			opt.Timeout = timeout
		case *PublicKeyCredentialRequestOptions:
			opt.Timeout = timeout
		default:
			return ErrOption.Wrap(NewError("Timeout must receive creation or request object"))
		}
		return nil
	}
}

//Extensions returns an option that adds one or more extensions to the
//creation options object or request options object
func Extensions(exts ...Extension) Option {
	return func(in interface{}) error {
		switch opt := in.(type) {
		case *PublicKeyCredentialCreationOptions:
			opt.Extensions = BuildExtensions(exts...)
		case *PublicKeyCredentialRequestOptions:
			opt.Extensions = BuildExtensions(exts...)
		default:
			return ErrOption.Wrap(NewError("Extensions must receive creation or request option"))
		}
		return nil
	}
}

//ExcludeCredentials returns an option that adds a list of credentials to
//exclude to the creation options object
func ExcludeCredentials(creds []PublicKeyCredentialDescriptor) Option {
	return func(in interface{}) error {
		if opt, ok := in.(*PublicKeyCredentialCreationOptions); ok {
			opt.ExcludeCredentials = creds
			return nil
		}
		return ErrOption.Wrap(NewError("ExcludeCredentials must receive creation object"))
	}
}

//AuthenticatorSelection returns an option that adds authenticator selection
//criteria to the creation options object
func AuthenticatorSelection(criteria AuthenticatorSelectionCriteria) Option {
	return func(in interface{}) error {
		if opt, ok := in.(*PublicKeyCredentialCreationOptions); ok {
			opt.AuthenticatorSelection = &criteria
			return nil
		}
		return ErrOption.Wrap(NewError("AuthenticatorSelection must receive creation object"))
	}
}

//Attestation returns an option that adds an attestation conveyance
//preference to the creation options object
func Attestation(pref AttestationConveyancePreference) Option {
	return func(in interface{}) error {
		if opt, ok := in.(*PublicKeyCredentialCreationOptions); ok {
			opt.Attestation = pref
			return nil
		}
		return ErrOption.Wrap(NewError("Attestation must receive creation object"))
	}
}

//RelyingPartyID returns an option that specifies the Relying Party ID in the
//credential request object
func RelyingPartyID(rpID string) Option {
	return func(in interface{}) error {
		if opt, ok := in.(*PublicKeyCredentialRequestOptions); ok {
			opt.RPID = rpID
			return nil
		}
		return ErrOption.Wrap(NewError("RelyingPartyID must receive request object"))
	}
}

//AllowCredentials returns an option that adds a list of allowed credentials to
//the credential request object
func AllowCredentials(creds []PublicKeyCredentialDescriptor) Option {
	return func(in interface{}) error {
		if opt, ok := in.(*PublicKeyCredentialRequestOptions); ok {
			opt.AllowCredentials = creds
			return nil
		}
		return ErrOption.Wrap(NewError("AllowCredentials must receive request object"))
	}
}

//UserVerification returns an option that adds the relying party argument for
//user verification to the credential request object
func UserVerification(req UserVerificationRequirement) Option {
	return func(in interface{}) error {
		if opt, ok := in.(*PublicKeyCredentialRequestOptions); ok {
			opt.UserVerification = req
			return nil
		}
		return ErrOption.Wrap(NewError("UserVerification must receive request object"))
	}
}
