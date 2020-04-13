package warp

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/x509"
	"encoding/asn1"

	"github.com/fxamacker/cbor"
)

var idFidoGenCeAaguid asn1.ObjectIdentifier = asn1.ObjectIdentifier([]int{1, 3, 6, 1, 4, 1, 45724, 1, 1, 4})

//AttestationObject contains both authenticator data and an attestation
//statement.
type AttestationObject struct {
	AuthData AuthenticatorData
	Fmt      AttestationStatementFormat
	AttStmt  cbor.RawMessage
}

type encodingAttObj struct {
	AuthData []byte                     `cbor:"authData"`
	Fmt      AttestationStatementFormat `cbor:"fmt"`
	AttStmt  cbor.RawMessage            `cbor:"attStmt"`
}

//MarshalBinary implements the BinaryMarshaler interface, and returns the raw
//CBOR encoding of AttestationObject
func (ao *AttestationObject) MarshalBinary() (data []byte, err error) {
	rawAuthData, _ := (&ao.AuthData).MarshalBinary() //cannot fail

	intermediate := encodingAttObj{
		AuthData: rawAuthData,
		Fmt:      ao.Fmt,
		AttStmt:  ao.AttStmt,
	}

	return cbor.Marshal(&intermediate, cbor.CTAP2EncOptions())
}

//UnmarshalBinary implements the BinaryUnmarshaler interface, and populates an
//AttestationObject with the provided raw CBOR
func (ao *AttestationObject) UnmarshalBinary(data []byte) error {
	intermediate := encodingAttObj{}
	if err := cbor.Unmarshal(data, &intermediate); err != nil {
		return ErrUnmarshalAttestationObject.Wrap(err)
	}

	if err := (&ao.AuthData).UnmarshalBinary(intermediate.AuthData); err != nil {
		return ErrUnmarshalAttestationObject.Wrap(err)
	}

	ao.Fmt = intermediate.Fmt
	ao.AttStmt = intermediate.AttStmt
	return nil
}

//AttestationStatementFormat is the identifier for an attestation statement
//format.
type AttestationStatementFormat string

//enum values for AttestationStatementFormat
const (
	AttestationFormatPacked           AttestationStatementFormat = "packed"
	AttestationFormatTPM              AttestationStatementFormat = "tpm"
	AttestationFormatAndroidKey       AttestationStatementFormat = "android-key"
	AttestationFormatAndroidSafetyNet AttestationStatementFormat = "android-safetynet"
	AttestationFormatFidoU2F          AttestationStatementFormat = "fido-u2f"
	AttestationFormatNone             AttestationStatementFormat = "none"
)

//Valid determines if the Attestation Format Identifier is a valid value
func (asf AttestationStatementFormat) Valid() error {
	switch asf {
	case AttestationFormatPacked:
	case AttestationFormatTPM:
	case AttestationFormatAndroidKey:
	case AttestationFormatAndroidSafetyNet:
	case AttestationFormatFidoU2F:
	case AttestationFormatNone:
	default:
		return NewError("Invalid attestation statement %s", asf)
	}
	return nil
}

//VerifyNoneAttestationStatement verifies that at attestation statement of type
//"none" is valid
func VerifyNoneAttestationStatement(attStmt []byte, _ []byte, _ [32]byte) error {
	if !bytes.Equal([]byte(attStmt), []byte{0xa0}) { //empty map
		return ErrVerifyAttestation.Wrap(NewError("Attestation format none with non-empty statement: %#v", attStmt))
	}
	return nil
}

//PackedAttestationStatement represents a decoded attestation statement of type
//"packed"
type PackedAttestationStatement struct {
	Alg        COSEAlgorithmIdentifier `cbor:"alg"`
	Sig        []byte                  `cbor:"sig"`
	X5C        [][]byte                `cbor:"x5c"`
	ECDAAKeyID []byte                  `cbor:"ecdaaKeyId"`
}

var coseToSigAlg = map[COSEAlgorithmIdentifier]x509.SignatureAlgorithm{
	AlgorithmES256: x509.ECDSAWithSHA256,
	AlgorithmES384: x509.ECDSAWithSHA384,
	AlgorithmES512: x509.ECDSAWithSHA512,
	AlgorithmEdDSA: x509.PureEd25519,
	AlgorithmPS256: x509.SHA256WithRSAPSS,
	AlgorithmPS384: x509.SHA384WithRSAPSS,
	AlgorithmPS512: x509.SHA512WithRSAPSS,
	AlgorithmRS1:   x509.SHA1WithRSA,
	AlgorithmRS256: x509.SHA256WithRSA,
	AlgorithmRS384: x509.SHA384WithRSA,
	AlgorithmRS512: x509.SHA512WithRSA,
}

//VerifyPackedAttestationStatement verifies that an attestation statement of
//type "packed" is valid
func VerifyPackedAttestationStatement(attStmt []byte, rawAuthData []byte, clientDataHash [32]byte) error {
	//1. Verify that attStmt is valid CBOR conforming to the syntax defined
	//above and perform CBOR decoding on it to extract the contained fields.
	var att PackedAttestationStatement
	if err := cbor.Unmarshal(attStmt, &att); err != nil {
		return ErrVerifyAttestation.Wrap(NewError("packed attestation statement not valid CBOR").Wrap(err))
	}

	authData := AuthenticatorData{}
	if err := authData.UnmarshalBinary(rawAuthData); err != nil {
		return ErrVerifyAttestation.Wrap(NewError("error unmarshaling authData"))
	}

	verificationData := append(rawAuthData, clientDataHash[:]...)

	if len(att.X5C) > 0 {
		//2. If x5c is present, this indicates that the attestation type is not
		//ECDAA. In this case:

		//Verify that sig is a valid signature over the concatenation of
		//authenticatorData and clientDataHash using the attestation public key in
		//attestnCert with the algorithm specified in alg.
		attestnCert, err := x509.ParseCertificate(att.X5C[0])
		if err != nil {
			return ErrVerifyAttestation.Wrap(NewError("error parsing attestation certificate").Wrap(err))
		}

		sigAlg, ok := coseToSigAlg[att.Alg]
		if !ok {
			return ErrVerifyAttestation.Wrap(NewError("unsupported signature algorithm").Wrap(err))
		}
		if err = attestnCert.CheckSignature(sigAlg, verificationData, att.Sig); err != nil {
			return ErrVerifyAttestation.Wrap(NewError("error verifying signature over auth and client data").Wrap(err))
		}

		//Verify that attestnCert meets the requirements in §8.2.1 Packed
		//Attestation Statement Certificate Requirements.

		//Version MUST be set to 3 (which is indicated by an ASN.1 INTEGER with
		//value 2).
		if attestnCert.Version != 3 {
			return ErrVerifyAttestation.Wrap(NewError("invalid attestation certificate version"))
		}

		//Subject field MUST be set to:
		//
		// * Subject-C
		// ISO 3166 code specifying the country where the Authenticator vendor is incorporated (PrintableString)
		// * Subject-O
		// Legal name of the Authenticator vendor (UTF8String)
		// * Subject-OU
		// Literal string “Authenticator Attestation” (UTF8String)
		// * Subject-CN
		// A UTF8String of the vendor’s choosing
		if len(attestnCert.Subject.Country) < 1 || len(attestnCert.Subject.Country[0]) < 2 || len(attestnCert.Subject.Country[0]) > 3 {
			return ErrVerifyAttestation.Wrap(NewError("invalid subject country, must be ISO3166 code"))
		}
		if len(attestnCert.Subject.Organization) < 1 {
			return ErrVerifyAttestation.Wrap(NewError("subject organization not present"))
		}
		if len(attestnCert.Subject.OrganizationalUnit) < 1 || attestnCert.Subject.OrganizationalUnit[0] != "Authenticator Attestation" {
			return ErrVerifyAttestation.Wrap(NewError("invalid subject organizational unit, must be \"Authenticator Attestation\""))
		}
		if attestnCert.Subject.CommonName == "" {
			return ErrVerifyAttestation.Wrap(NewError("subject common name not present"))
		}

		//If the related attestation root certificate is used for multiple authenticator models, the Extension OID
		//1.3.6.1.4.1.45724.1.1.4 (id-fido-gen-ce-aaguid) MUST be present, containing the AAGUID as a 16-byte OCTET
		//STRING. The extension MUST NOT be marked as critical.
		for _, ext := range attestnCert.Extensions {
			if ext.Id.Equal(idFidoGenCeAaguid) {
				if ext.Critical {
					return ErrVerifyAttestation.Wrap(NewError("AAGUID extension marked critical"))
				}
				var certAAGUID []byte
				_, err := asn1.Unmarshal(ext.Value, &certAAGUID)
				if err != nil {
					return ErrVerifyAttestation.Wrap(NewError("error unmarshaling certificate AAGUID").Wrap(err))
				}

				if !bytes.Equal(certAAGUID, authData.AttestedCredentialData.AAGUID[:]) {
					return ErrVerifyAttestation.Wrap(NewError("AAGUID mismatch"))
				}
			}
		}

		//The Basic Constraints extension MUST have the CA component set to false.
		if attestnCert.IsCA {
			return ErrVerifyAttestation.Wrap(NewError("attestation certificate has CA constraint"))
		}
	} else if att.ECDAAKeyID != nil {
		//3. If ecdaaKeyId is present, then the attestation type is ECDAA.
		return ErrVerifyAttestation.Wrap(ErrECDAANotSupported)
	} else {
		//4. If neither x5c nor ecdaaKeyId is present, self attestation is in
		//use.

		//Validate that alg matches the algorithm of the credentialPublicKey in
		//authenticatorData.
		var credPubKey COSEKey
		if err := cbor.Unmarshal(authData.AttestedCredentialData.CredentialPublicKey, &credPubKey); err != nil {
			return ErrVerifyAttestation.Wrap(NewError("error unmarshaling credential public key").Wrap(err))
		}
		if credPubKey.Alg != int(att.Alg) {
			return ErrVerifyAttestation.Wrap(NewError("credential public key algorithm does not match attestation algorithm"))
		}

		//Verify that sig is a valid signature over the concatenation of
		//authenticatorData and clientDataHash using the credential public key
		//with alg.
		if err := VerifySignature(authData.AttestedCredentialData.CredentialPublicKey, verificationData, att.Sig); err != nil {
			return ErrVerifyAttestation.Wrap(err)
		}
	}

	return nil
}

//FIDOU2FAttestationStatement represents a decoded attestation statement of type
//"fido-u2f"
type FIDOU2FAttestationStatement struct {
	X5C [][]byte `cbor:"x5c"`
	Sig []byte   `cbor:"sig"`
}

//VerifyFIDOU2FAttestationStatement verifies that an attestation statement of
//type "fido-u2f" is valid
func VerifyFIDOU2FAttestationStatement(attStmt []byte, rawAuthData []byte, clientDataHash [32]byte) error {
	//1. Verify that attStmt is valid CBOR conforming to the syntax defined
	//above and perform CBOR decoding on it to extract the contained fields.
	var att FIDOU2FAttestationStatement
	if err := cbor.Unmarshal(attStmt, &att); err != nil {
		return ErrVerifyAttestation.Wrap(NewError("fido-u2f attestation statement not valid CBOR").Wrap(err))
	}

	//2. Check that x5c has exactly one element and let attCert be that element.
	//Let certificate public key be the public key conveyed by attCert. If
	//certificate public key is not an Elliptic Curve (EC) public key over the
	//P-256 curve, terminate this algorithm and return an appropriate error.
	if len(att.X5C) != 1 {
		return ErrVerifyAttestation.Wrap(NewError("x5c has %d members, expected 1", len(att.X5C)))
	}
	cert, err := x509.ParseCertificate(att.X5C[0])
	if err != nil {
		return ErrVerifyAttestation.Wrap(NewError("error parsing attestation certificate"))
	}
	publicKey, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		return ErrVerifyAttestation.Wrap(NewError("certificate public key not ecdsa"))
	}
	if publicKey.Curve != elliptic.P256() {
		return ErrVerifyAttestation.Wrap(NewError("certificate public key not on P-256 curve"))
	}

	//3. Extract the claimed rpIdHash from authenticatorData, and the claimed
	//credentialId and credentialPublicKey from
	//authenticatorData.attestedCredentialData.
	var authData AuthenticatorData
	if err := (&authData).UnmarshalBinary(rawAuthData); err != nil {
		return ErrVerifyAttestation.Wrap(NewError("error parsing auth data").Wrap(err))
	}

	//4. Convert the COSE_KEY formatted credentialPublicKey (see Section 7 of
	//[RFC8152]) to Raw ANSI X9.62 public key format (see ALG_KEY_ECC_X962_RAW
	//in Section 3.6.2 Public Key Representation Formats of [FIDO-Registry]).
	var cosePublicKey COSEKey
	if err := cbor.Unmarshal(authData.AttestedCredentialData.CredentialPublicKey, &cosePublicKey); err != nil {
		return ErrVerifyAttestation.Wrap(NewError("error parsing credential public key").Wrap(err))
	}

	//Let x be the value corresponding to the "-2" key (representing x
	//coordinate) in credentialPublicKey, and confirm its size to be of 32
	//bytes. If size differs or "-2" key is not found, terminate this algorithm
	//and return an appropriate error.
	var x, y []byte
	if err := cbor.Unmarshal(cosePublicKey.XOrE, &x); err != nil {
		return ErrVerifyAttestation.Wrap(NewError("error parsing public key x parameter").Wrap(err))
	}
	if len(x) != 32 {
		return ErrVerifyAttestation.Wrap(NewError("unexpected length %d for public key x param", len(x)))
	}

	//Let y be the value corresponding to the "-3" key (representing y
	//coordinate) in credentialPublicKey, and confirm its size to be of 32
	//bytes. If size differs or "-3" key is not found, terminate this algorithm
	//and return an appropriate error.
	if err := cbor.Unmarshal(cosePublicKey.Y, &y); err != nil {
		return ErrVerifyAttestation.Wrap(NewError("error parsing public key y parameter").Wrap(err))
	}
	if len(y) != 32 {
		return ErrVerifyAttestation.Wrap(NewError("unexpected length %d for public key y param", len(y)))
	}

	//Let publicKeyU2F be the concatenation 0x04 || x || y.
	publicKeyU2F := append(append([]byte{0x04}, x...), y...)

	//Let verificationData be the concatenation of (0x00 || rpIdHash ||
	//clientDataHash || credentialId || publicKeyU2F) (see Section 4.3 of
	//[FIDO-U2F-Message-Formats]).
	verificationData := append(
		append(
			append(
				append(
					[]byte{0x00}, authData.RPIDHash[:]...,
				), clientDataHash[:]...,
			), authData.AttestedCredentialData.CredentialID...,
		), publicKeyU2F...,
	)

	if err = cert.CheckSignature(x509.ECDSAWithSHA256, verificationData, att.Sig); err != nil {
		return ErrVerifyAttestation.Wrap(NewError("error verifying certificate").Wrap(err))
	}

	return nil
}
