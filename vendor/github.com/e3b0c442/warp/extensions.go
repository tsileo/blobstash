package warp

import (
	"bytes"
	"crypto"
	"encoding/base64"
	"hash"
	"strings"
)

//Identifiers for defined extensions
const (
	ExtensionAppID               = "appid"
	ExtensionTxAuthSimple        = "txAuthSimple"
	ExtensionTxAuthGeneric       = "txAuthGeneric"
	ExtensionAuthnSel            = "authnSel"
	ExtensionExts                = "exts"
	ExtensionUVI                 = "uvi"
	ExtensionLoc                 = "loc"
	ExtensionUVM                 = "uvm"
	ExtensionBiometricPerfBounds = "biometricPerfBounds"
)

//AuthenticationExtensionsClientInputs contains the client extension input
//values for zero or more extensions. §5.7
type AuthenticationExtensionsClientInputs map[string]interface{}

//AuthenticationExtensionsClientOutputs containing the client extension output
//values for zero or more WebAuthn extensions. §5.8
type AuthenticationExtensionsClientOutputs map[string]interface{}

//Extension defines an extension to a creation options or request options
//object
type Extension func(AuthenticationExtensionsClientInputs)

//BuildExtensions builds the extension map to be added to the options object
func BuildExtensions(exts ...Extension) AuthenticationExtensionsClientInputs {
	extensions := make(AuthenticationExtensionsClientInputs)

	for _, ext := range exts {
		ext(extensions)
	}

	return extensions
}

//UseAppID adds the appid extension to the extensions object
func UseAppID(appID string) Extension {
	return func(e AuthenticationExtensionsClientInputs) {
		e[ExtensionAppID] = appID
	}
}

//UseTxAuthSimple adds the txAuthSimple extension to the extensions object
func UseTxAuthSimple(txAuthSimple string) Extension {
	return func(e AuthenticationExtensionsClientInputs) {
		e[ExtensionTxAuthSimple] = txAuthSimple
	}
}

//UseTxAuthGeneric adds the txAuthGeneric extension to the extensions object
func UseTxAuthGeneric(contentType string, content []byte) Extension {
	return func(e AuthenticationExtensionsClientInputs) {
		e[ExtensionTxAuthGeneric] = map[string]interface{}{
			"contentType": contentType,
			"content":     content,
		}
	}
}

//RegistrationExtensionValidators is a map to all extension validators for
//extensions allowed during the registration ceremony
var RegistrationExtensionValidators map[string]RegistrationValidator = map[string]RegistrationValidator{}

//AuthenticationExtensionValidators is a map to all extension validators for
//extensions allowed during the authentication ceremony
var AuthenticationExtensionValidators map[string]AuthenticationValidator = map[string]AuthenticationValidator{
	ExtensionAppID:        ValidateAppID(),
	ExtensionTxAuthSimple: ValidateTxAuthSimple(),
}

//ValidateAppID validates the appid extension and updates the credential
//request options with the valid AppID as needed
func ValidateAppID() AuthenticationValidator {
	return func(opts *PublicKeyCredentialRequestOptions, cred *AssertionPublicKeyCredential) error {
		o, ok := cred.Extensions[ExtensionAppID]
		if !ok {
			return nil // do not fail on client ignored extension
		}
		i, ok := opts.Extensions[ExtensionAppID]
		if !ok {
			return ErrVerifyClientExtensionOutput.Wrap(NewError("appid extension present in credential but not requested in options"))
		}

		out, ok := o.(bool)
		if !ok {
			return ErrVerifyClientExtensionOutput.Wrap(NewError("unexpected type on appid extension output"))
		}
		in, ok := i.(string)
		if !ok {
			return ErrVerifyClientExtensionOutput.Wrap(NewError("unexpected type on appid extension input"))
		}

		if out {
			opts.RPID = in
		}
		return nil
	}
}

//ValidateTxAuthSimple validates the txAuthSimple extension
func ValidateTxAuthSimple() AuthenticationValidator {
	return func(opts *PublicKeyCredentialRequestOptions, cred *AssertionPublicKeyCredential) error {
		o, ok := cred.Extensions[ExtensionTxAuthSimple]
		if !ok {
			return nil // do not fail on client ignored extension
		}
		i, ok := opts.Extensions[ExtensionTxAuthSimple]
		if !ok {
			return ErrVerifyClientExtensionOutput.Wrap(NewError("txAuthSimple extension present in credential but not requested in options"))
		}
		out, ok := o.(string)
		if !ok {
			return ErrVerifyClientExtensionOutput.Wrap(NewError("unexpected type on txAuthSimple extension output"))
		}
		in, ok := i.(string)
		if !ok {
			return ErrVerifyClientExtensionOutput.Wrap(NewError("unexpected type on txAuthSimple extension input"))
		}
		if strings.ReplaceAll(in, "\n", "") != strings.ReplaceAll(out, "\n", "") {
			return ErrVerifyClientExtensionOutput.Wrap(NewError("txAuthSimple output differs from input"))
		}
		return nil
	}
}

//ValidateTxAuthGeneric validates the txAuthGeneric extension
func ValidateTxAuthGeneric() AuthenticationValidator {
	return func(opts *PublicKeyCredentialRequestOptions, cred *AssertionPublicKeyCredential) error {
		o, ok := cred.Extensions[ExtensionTxAuthGeneric]
		if !ok {
			return nil // do not fail on client ignored extension
		}
		i, ok := opts.Extensions[ExtensionTxAuthGeneric]
		if !ok {
			return ErrVerifyClientExtensionOutput.Wrap(NewError("txAuthSimple extension present in credential but not requested in options"))
		}
		outB64, ok := o.(string)
		if !ok {
			return ErrVerifyClientExtensionOutput.Wrap(NewError("unexpected type on txAuthGeneric extension output"))
		}
		out, err := base64.StdEncoding.DecodeString(outB64)
		if err != nil {
			return ErrVerifyClientExtensionOutput.Wrap(NewError("unable to decode txAuthGeneric extension output"))
		}
		in, ok := i.(map[string]interface{})
		if !ok {
			return ErrVerifyClientExtensionOutput.Wrap(NewError("unexpected type on txAuthGeneric extension input"))
		}
		if _, ok := in["contentType"]; !ok {
			return ErrVerifyClientExtensionOutput.Wrap(NewError("txAuthGeneric input missing contentType member"))
		}
		if _, ok := in["contentType"].(string); !ok {
			return ErrVerifyClientExtensionOutput.Wrap(NewError("txAuthGeneric input contentType invalid type"))
		}
		if _, ok := in["content"]; !ok {
			return ErrVerifyClientExtensionOutput.Wrap(NewError("txAuthGeneric input missing content member"))
		}
		inBytes, ok := in["content"].([]byte)
		if !ok {
			return ErrVerifyClientExtensionOutput.Wrap(NewError("txAuthGeneric input content invalid type"))
		}

		var hasher hash.Hash
		switch len(out) {
		case crypto.SHA1.Size():
			hasher = crypto.SHA1.New()
		case crypto.SHA256.Size():
			hasher = crypto.SHA256.New()
		case crypto.SHA384.Size():
			hasher = crypto.SHA384.New()
		case crypto.SHA512.Size():
			hasher = crypto.SHA512.New()
		default:
			return ErrVerifyClientExtensionOutput.Wrap(NewError("txAuthGeneric output digest unknown length"))
		}
		hasher.Write(inBytes)
		if !bytes.Equal(hasher.Sum(nil), out) {
			return ErrVerifyClientExtensionOutput.Wrap(NewError("txAuthGeneric returned hash does not match input"))
		}

		return nil
	}
}
