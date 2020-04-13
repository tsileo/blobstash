package warp

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"strings"
)

var randReader io.Reader = rand.Reader

func generateChallenge() ([]byte, error) {
	challenge := make([]byte, ChallengeLength)
	n, err := randReader.Read(challenge)
	if err != nil {
		return nil, err
	}
	if n < ChallengeLength {
		return nil, NewError("Read %d random bytes, needed %d", n, ChallengeLength)
	}

	return challenge, nil
}

func decodeAuthData(raw []byte) (*AuthenticatorData, error) {
	var authData AuthenticatorData
	err := authData.Decode(bytes.NewBuffer(raw))
	if err != nil {
		return nil, err
	}
	return &authData, nil
}

func parseClientData(jsonText []byte) (*CollectedClientData, error) {
	C := CollectedClientData{}
	err := json.Unmarshal(jsonText, &C)
	if err != nil {
		return nil, NewError("Error unmarshaling client data JSON").Wrap(err)
	}
	return &C, nil
}

func verifyChallenge(C *CollectedClientData, challenge []byte) error {
	rawChallenge, err := base64.RawURLEncoding.DecodeString(C.Challenge)
	if err != nil {
		return err
	}

	if !bytes.Equal(rawChallenge, challenge) {
		return NewError("Challenge mismatch: got [% X] expected [% X]", rawChallenge, challenge)
	}
	return nil
}

func verifyOrigin(C *CollectedClientData, rp RelyingParty) error {
	if !strings.EqualFold(rp.Origin(), C.Origin) {
		return NewError("Origin mismatch: got %s, expected %s", C.Origin, rp.Origin())
	}
	return nil
}

func verifyTokenBinding(C *CollectedClientData) error {
	if C.TokenBinding != nil {
		switch C.TokenBinding.Status {
		case StatusSupported:
		case StatusPresent:
			if C.TokenBinding.ID == "" {
				return NewError("Token binding status present without ID")
				//TODO implement Token Binding validation when support exists in
				//Golang standard library
			}
		default:
			return NewError("Invalid token binding status %s", C.TokenBinding.Status)
		}
	}
	return nil
}

func verifyRPIDHash(RPID string, authData *AuthenticatorData) error {
	rpIDHash := sha256.Sum256([]byte(RPID))
	if !bytes.Equal(rpIDHash[:], authData.RPIDHash[:]) {
		return NewError("RPID hash does not match authData (RPID: %s)", RPID)
	}
	return nil
}

func verifyUserPresent(authData *AuthenticatorData) error {
	if !authData.UP {
		return NewError("User Present bit not set")
	}
	return nil
}

func verifyUserVerified(authData *AuthenticatorData) error {
	if !authData.UV {
		return NewError("User Verification required but missing")
	}
	return nil
}

func verifyClientExtensionsOutputs(ins AuthenticationExtensionsClientInputs, outs AuthenticationExtensionsClientOutputs) error {
	for k := range outs {
		_, ok := ins[k]
		if !ok {
			return NewError("Extension key %s provided in credential but not options", k)
		}
	}
	return nil
}
