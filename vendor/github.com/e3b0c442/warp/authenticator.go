package warp

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/fxamacker/cbor/v2"
)

//AuthenticatorData encodes contextual bindings made by the authenticator.
type AuthenticatorData struct {
	RPIDHash               [32]byte
	UP                     bool
	UV                     bool
	AT                     bool
	ED                     bool
	SignCount              uint32
	AttestedCredentialData AttestedCredentialData
	Extensions             map[string]interface{}
}

//MarshalBinary implements the BinaryMarshaler interface, and returns the raw
//binary authData
func (ad *AuthenticatorData) MarshalBinary() (data []byte, err error) {
	b := &bytes.Buffer{}
	ad.Encode(b) //writes to bytes.Buffer cannot fail
	return b.Bytes(), nil
}

//UnmarshalBinary implements the BinaryUnmarshaler interface, and populates an
//AuthenticatorData with the provided raw authData
func (ad *AuthenticatorData) UnmarshalBinary(data []byte) error {
	return ad.Decode(bytes.NewBuffer(data))
}

//Encode encodes the AuthenticatorData structure into the raw binary authData
func (ad *AuthenticatorData) Encode(w io.Writer) error {
	n, err := w.Write(ad.RPIDHash[:])
	if err != nil {
		return ErrEncodeAuthenticatorData.Wrap(NewError("Unable to write RPIDHash").Wrap(err))
	}
	if n != 32 {
		return ErrEncodeAuthenticatorData.Wrap(NewError("RPIDHash Wrote %d bytes, needed 32", n))
	}

	var flags uint8
	if ad.UP {
		flags = flags | 0x01
	}
	if ad.UV {
		flags = flags | 0x04
	}
	if ad.AT {
		flags = flags | 0x40
	}
	if ad.ED {
		flags = flags | 0x80
	}

	_, err = w.Write([]byte{flags})
	if err != nil {
		return ErrEncodeAuthenticatorData.Wrap(NewError("Unable to write flags").Wrap(err))
	}

	err = binary.Write(w, binary.BigEndian, ad.SignCount)
	if err != nil {
		return ErrEncodeAuthenticatorData.Wrap(NewError("Error writing sign count").Wrap(err))
	}

	if ad.AT {
		err = ad.AttestedCredentialData.Encode(w)
		if err != nil {
			return ErrEncodeAuthenticatorData.Wrap(NewError("Error writing attested credential data").Wrap(err))
		}
	}

	if ad.ED {
		em, _ := cbor.CTAP2EncOptions().EncMode()
		err = em.NewEncoder(w).Encode(ad.Extensions)
		if err != nil {
			return ErrEncodeAuthenticatorData.Wrap(NewError("Error writing extensions").Wrap(err))
		}
	}

	return nil
}

//Decode decodes the ad hoc AuthenticatorData structure
func (ad *AuthenticatorData) Decode(data io.Reader) error {
	n, err := data.Read(ad.RPIDHash[:])
	if err != nil {
		return ErrDecodeAuthenticatorData.Wrap(NewError("Error reanding relying party ID hash").Wrap(err))
	}
	if n < 32 {
		return ErrDecodeAuthenticatorData.Wrap(NewError("Expected 32 bytes of hash data, got %d", n))
	}

	var flags uint8
	err = binary.Read(data, binary.BigEndian, &flags)
	if err != nil {
		return ErrDecodeAuthenticatorData.Wrap(NewError("Error reading flag byte").Wrap(err))
	}

	ad.UP = false
	ad.UV = false
	ad.AT = false
	ad.ED = false
	if flags&0x1 > 0 {
		ad.UP = true
	}
	if flags&0x4 > 0 {
		ad.UV = true
	}
	if flags&0x40 > 0 {
		ad.AT = true
	}
	if flags&0x80 > 0 {
		ad.ED = true
	}

	err = binary.Read(data, binary.BigEndian, &ad.SignCount)
	if err != nil {
		return ErrDecodeAuthenticatorData.Wrap(NewError("Error reading sign count").Wrap(err))
	}

	if ad.AT {
		err = ad.AttestedCredentialData.Decode(data)
		if err != nil {
			return ErrDecodeAuthenticatorData.Wrap(err)
		}
	}

	if ad.ED {
		err = cbor.NewDecoder(data).Decode(&ad.Extensions)
		if err != nil {
			return ErrDecodeAuthenticatorData.Wrap(err)
		}
	}

	return nil
}

//AttestedCredentialData is a variable-length byte array added to the
//authenticator data when generating an attestation object for a given
//credential. §6.4.1
type AttestedCredentialData struct {
	AAGUID              [16]byte
	CredentialID        []byte
	CredentialPublicKey cbor.RawMessage
}

//Decode decodes the attested credential data from a stream
func (acd *AttestedCredentialData) Decode(data io.Reader) error {
	n, err := data.Read(acd.AAGUID[:])
	if err != nil {
		return ErrDecodeAttestedCredentialData.Wrap(NewError("Error reading AAGUID").Wrap(err))
	}
	if n < 16 {
		return ErrDecodeAttestedCredentialData.Wrap(NewError("Expected 16 bytes of AAGUID data, got %d", n))
	}

	var credLen uint16
	err = binary.Read(data, binary.BigEndian, &credLen)
	if err != nil {
		return ErrDecodeAttestedCredentialData.Wrap(NewError("Error reading credential length").Wrap(err))
	}

	acd.CredentialID = make([]byte, credLen)
	_, err = data.Read(acd.CredentialID)
	if err != nil {
		return ErrDecodeAttestedCredentialData.Wrap(NewError("Error reading credential ID").Wrap(err))
	}

	err = cbor.NewDecoder(data).Decode(&acd.CredentialPublicKey)
	if err != nil {
		return ErrDecodeAttestedCredentialData.Wrap(NewError("Error unmarshaling COSE key data").Wrap(err))
	}

	return nil
}

//Encode encodes the attested credential data to a stream
func (acd *AttestedCredentialData) Encode(w io.Writer) error {
	n, err := w.Write(acd.AAGUID[:])
	if err != nil {
		return ErrEncodeAttestedCredentialData.Wrap(NewError("Error writing AAGUID").Wrap(err))
	}
	if n < 16 {
		return ErrEncodeAttestedCredentialData.Wrap(NewError("AAGUID wrote %d bytes, needed 16", n))
	}

	var credLen = uint16(len(acd.CredentialID))
	err = binary.Write(w, binary.BigEndian, credLen)
	if err != nil {
		return ErrEncodeAttestedCredentialData.Wrap(NewError("Error writing credential ID length").Wrap(err))
	}

	n, err = w.Write(acd.CredentialID)
	if err != nil {
		return ErrEncodeAttestedCredentialData.Wrap(NewError("Error writing credential ID").Wrap(err))
	}
	if uint16(n) != credLen {
		return ErrEncodeAttestedCredentialData.Wrap(NewError("CredentialID wrote %d bytes, needed %d", n, credLen))
	}
	em, _ := cbor.CTAP2EncOptions().EncMode()
	err = em.NewEncoder(w).Encode(acd.CredentialPublicKey)
	if err != nil {
		return ErrEncodeAttestedCredentialData.Wrap(NewError("Error writing CredentialPublicKey").Wrap(err))
	}
	return nil
}
