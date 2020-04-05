// Base64 to ArrayBuffer
let bufferDecode = value => {
  return Uint8Array.from(atob(value), c => c.charCodeAt(0));
}
 
// ArrayBuffer to URLBase64
let bufferEncode = value => {
  return btoa(String.fromCharCode.apply(null, new Uint8Array(value)))
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=/g, "");;
}

// Webauthn module that makes it easy to work with BlobStash apps Webauthn API
var Webauthn = {
  login: (credentialRequestOptions, cb) => { 
    credentialRequestOptions.publicKey.challenge = bufferDecode(credentialRequestOptions.publicKey.challenge);
    credentialRequestOptions.publicKey.allowCredentials.forEach(listItem => { listItem.id = bufferDecode(listItem.id) });

    navigator.credentials.get({
      publicKey: credentialRequestOptions.publicKey,
    }).then(assertion => {
      let authData = assertion.response.authenticatorData;
      let clientDataJSON = assertion.response.clientDataJSON;
      let rawId = assertion.rawId;
      let sig = assertion.response.signature;
      let userHandle = assertion.response.userHandle;
      let payload = {
        id: assertion.id,
        rawId: bufferEncode(rawId),
        type: assertion.type,
        response: { 
          authenticatorData: bufferEncode(authData),
          clientDataJSON: bufferEncode(clientDataJSON),
          signature: bufferEncode(sig),
          userHandle: bufferEncode(userHandle),
        },
      };
      cb(payload);
    });
  },
  register: (credentialCreationOptions, cb) => {
    credentialCreationOptions.publicKey.challenge = bufferDecode(credentialCreationOptions.publicKey.challenge);
    credentialCreationOptions.publicKey.user.id = bufferDecode(credentialCreationOptions.publicKey.user.id);
    if (credentialCreationOptions.publicKey.excludeCredentials) {
      for (var i = 0; i < credentialCreationOptions.publicKey.excludeCredentials.length; i++) {
        credentialCreationOptions.publicKey.excludeCredentials[i].id = bufferDecode(credentialCreationOptions.publicKey.excludeCredentials[i].id);
      }
    }

    navigator.credentials.create({
      publicKey: credentialCreationOptions.publicKey
    }).then(credential => {
      let attestationObject = credential.response.attestationObject;
      let clientDataJSON = credential.response.clientDataJSON;
      let rawId = credential.rawId;
      let payload = {
        id: credential.id,
        rawId: bufferEncode(rawId),
        type: credential.type,
        response: {
          attestationObject: bufferEncode(attestationObject),
          clientDataJSON: bufferEncode(clientDataJSON),
	    },
      };
      cb(payload);
    });
  },
}
