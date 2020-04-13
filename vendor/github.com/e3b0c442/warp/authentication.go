package warp

import (
	"bytes"
	"crypto/sha256"
)

//StartAuthentication starts the authentication ceremony by creating a
//credential request options object to be sent to the client
func StartAuthentication(
	opts ...Option,
) (
	*PublicKeyCredentialRequestOptions,
	error,
) {
	challenge, err := generateChallenge()
	if err != nil {
		return nil, ErrGenerateChallenge.Wrap(err)
	}

	requestOptions := PublicKeyCredentialRequestOptions{
		Challenge: challenge,
	}

	for _, opt := range opts {
		err = opt(&requestOptions)
		if err != nil {
			return nil, err
		}
	}

	return &requestOptions, nil
}

//FinishAuthentication completes the authentication ceremony by validating the
//provided credential assertion against the stored public key.
func FinishAuthentication(
	rp RelyingParty,
	userFinder UserFinder,
	opts *PublicKeyCredentialRequestOptions,
	cred *AssertionPublicKeyCredential,
	vals ...AuthenticationValidator,
) (*AuthenticatorData, error) {
	//0. NON-NORMATIVE run all additional validators provided as args.
	for _, val := range vals {
		if err := val(opts, cred); err != nil {
			return nil, ErrVerifyAuthentication.Wrap(err)
		}
	}

	//1. If the allowCredentials option was given when this authentication
	//ceremony was initiated, verify that credential.id identifies one of the
	//public key credentials that were listed in allowCredentials.
	if err := checkAllowedCredentials(opts.AllowCredentials, cred.RawID); err != nil {
		return nil, ErrVerifyAuthentication.Wrap(err)
	}

	//2. Identify the user being authenticated and verify that this user is the
	//owner of the public key credential source credentialSource identified by
	//credential.id. If the user was identified before the authentication
	//ceremony was initiated, verify that the identified user is the owner of
	//credentialSource. If credential.response.userHandle is present, verify
	//that this value identifies the same user as was previously identified. If
	//the user was not identified before the authentication ceremony was
	//initiated, verify that credential.response.userHandle is present, and that
	//the user identified by this value is the owner of credentialSource.
	//Combined with step 3

	//3. Using credential’s id attribute (or the corresponding rawId, if
	//base64url encoding is inappropriate for your use case), look up the
	//corresponding credential public key.
	storedCred, err := getUserVerifiedCredential(userFinder, cred)
	if err != nil {
		return nil, ErrVerifyAuthentication.Wrap(err)
	}

	//4. Let cData, authData and sig denote the value of credential’s response's
	//clientDataJSON, authenticatorData, and signature respectively.
	cData := cred.Response.ClientDataJSON
	rawAuthData := cred.Response.AuthenticatorData
	sig := cred.Response.Signature
	authData, err := decodeAuthData(rawAuthData)
	if err != nil {
		return nil, ErrVerifyAuthentication.Wrap(err)
	}

	//5. Let JSONtext be the result of running UTF-8 decode on the value of
	//cData.
	//TODO research if there are any instances where the byte stream is not
	//valid JSON per the JSON decoder

	//6. Let C, the client data claimed as used for the signature, be the result
	//of running an implementation-specific JSON parser on JSONtext.
	C, err := parseClientData(cData)
	if err != nil {
		return nil, ErrVerifyAuthentication.Wrap(err)
	}

	//7. Verify that the value of C.type is the string webauthn.get.
	if C.Type != "webauthn.get" {
		return nil, ErrVerifyAuthentication.Wrap(NewError("C.type is not webauthn.get"))
	}

	//8. Verify that the value of C.challenge matches the challenge that was
	//sent to the authenticator in the PublicKeyCredentialRequestOptions passed
	//to the get() call.
	if err = verifyChallenge(C, opts.Challenge); err != nil {
		return nil, ErrVerifyAuthentication.Wrap(err)
	}

	//9. Verify that the value of C.origin matches the Relying Party's origin.
	if err = verifyOrigin(C, rp); err != nil {
		return nil, ErrVerifyAuthentication.Wrap(err)
	}

	//10. Verify that the value of C.tokenBinding.status matches the state of
	//Token Binding for the TLS connection over which the attestation was
	//obtained. If Token Binding was used on that TLS connection, also verify
	//that C.tokenBinding.id matches the base64url encoding of the Token Binding
	//ID for the connection.
	if err = verifyTokenBinding(C); err != nil {
		return nil, ErrVerifyAuthentication.Wrap(err)
	}

	//11. Verify that the rpIdHash in authData is the SHA-256 hash of the RP ID
	//expected by the Relying Party.
	if opts.RPID == "" {
		opts.RPID = rp.EntityID()
	}
	if err = verifyRPIDHash(opts.RPID, authData); err != nil {
		return nil, ErrVerifyAuthentication.Wrap(err)
	}

	//12. Verify that the User Present bit of the flags in authData is set.
	if err = verifyUserPresent(authData); err != nil {
		return nil, ErrVerifyAuthentication.Wrap(err)
	}

	//13. If user verification is required for this assertion, verify that the
	//User Verified bit of the flags in authData is set.
	if opts.UserVerification == VerificationRequired {
		if err = verifyUserVerified(authData); err != nil {
			return nil, ErrVerifyAuthentication.Wrap(err)
		}
	}

	//14. Verify that the values of the client extension outputs in
	//clientExtensionResults and the authenticator extension outputs in the
	//extensions in authData are as expected, considering the client extension
	//input values that were given as the extensions option in the get() call.
	//In particular, any extension identifier values in the
	//clientExtensionResults and the extensions in authData MUST be also be
	//present as extension identifier values in the extensions member of
	//options, i.e., no extensions are present that were not requested. In the
	//general case, the meaning of "are as expected" is specific to the Relying
	//Party and which extensions are in use.
	//NON-NORMATIVE: We are only verifying the existence of keys is valid here;
	//to actually validate the extension an AuthenticationValidator must be
	//passed to this function.
	if err := verifyClientExtensionsOutputs(opts.Extensions, cred.Extensions); err != nil {
		return nil, ErrVerifyAuthentication.Wrap(err)
	}

	//15. Let hash be the result of computing a hash over the cData using
	//SHA-256.
	hash := sha256.Sum256(cData)

	//16. Using the credential public key looked up in step 3, verify that sig
	//is a valid signature over the binary concatenation of authData and hash.
	bincat := make([]byte, 0, sha256.Size+len(rawAuthData))
	bincat = append(bincat, rawAuthData...)
	bincat = append(bincat, hash[:]...)
	if err := VerifySignature(storedCred.CredentialPublicKey(), bincat, sig); err != nil {
		return nil, ErrVerifyAuthentication.Wrap(err)
	}

	//17. If the signature counter value authData.signCount is nonzero or the
	//value stored in conjunction with credential’s id attribute is nonzero,
	//then run the following sub-step:
	//If the signature counter value authData.signCount is:
	//greater than the signature counter value stored in conjunction with
	//credential’s id attribute.
	if uint(authData.SignCount) >= storedCred.CredentialSignCount() {
		// Update the stored signature counter value, associated with
		//credential’s id attribute, to be the value of authData.signCount.
		return authData, nil
	}
	//less than or equal to the signature counter value stored in
	//conjunction with credential’s id attribute.
	//This is a signal that the authenticator may be cloned, i.e. at least
	//two copies of the credential private key may exist and are being used
	//in parallel. Relying Parties should incorporate this information into
	//their risk scoring. Whether the Relying Party updates the stored
	//signature counter value in this case, or not, or fails the
	//authentication ceremony or not, is Relying Party-specific.
	return nil, ErrVerifyAuthentication.Wrap(NewError("Credential counter less than stored counter"))
}

func checkAllowedCredentials(allowed []PublicKeyCredentialDescriptor, id []byte) error {
	if len(allowed) == 0 {
		return nil
	}
	for _, cred := range allowed {
		if bytes.Equal(id, cred.ID) {
			return nil
		}
	}
	return NewError("Credential ID not found in allowed list")
}

func getUserVerifiedCredential(userFinder UserFinder, cred *AssertionPublicKeyCredential) (Credential, error) {
	user, err := userFinder(cred.Response.UserHandle)
	if err != nil {
		return nil, ErrVerifyAuthentication.Wrap(err)
	}

	storedCred, ok := user.Credentials()[cred.ID]
	if !ok {
		return nil, NewError("User %s does not own this credential", user.EntityName())
	}
	return storedCred, nil
}
