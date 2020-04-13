// Webauthn module that makes it easy to work with BlobStash apps Webauthn API
var Webauthn = {
  login: (credentialRequestOptions, cb) => { 
    credentialRequestOptions.challenge = Uint8Array.from(atob(credentialRequestOptions.challenge), c => c.charCodeAt(0))
    if(credentialRequestOptions.allowCredentials) {
      for (let i = 0; i < credentialRequestOptions.allowCredentials.length; i++) {
        credentialRequestOptions.allowCredentials[i].id = Uint8Array.from(atob(credentialRequestOptions.allowCredentials[i].id), c => c.charCodeAt(0))
      }
    }

    navigator.credentials.get({
      publicKey: credentialRequestOptions,
    }).then(opts => {
      const toSend = {
        id: opts.id,
        rawId: btoa(String.fromCharCode.apply(null, new Uint8Array(opts.rawId))),
        response: {
          authenticatorData: btoa(String.fromCharCode.apply(null, new Uint8Array(opts.response.authenticatorData))),
          signature: btoa(String.fromCharCode.apply(null, new Uint8Array(opts.response.signature))),
          clientDataJSON: btoa(String.fromCharCode.apply(null, new Uint8Array(opts.response.clientDataJSON))),
        },
        type: opts.type
      }
      if(opts.extensions) {
        toSend.extensions = opts.extensions
      }
      cb(toSend);
    });
  },
  register: (credentialCreationOptions, cb) => {
    credentialCreationOptions.user.id = Uint8Array.from(atob(credentialCreationOptions.user.id), c => c.charCodeAt(0))
    credentialCreationOptions.challenge = Uint8Array.from(atob(credentialCreationOptions.challenge), c => c.charCodeAt(0))

    navigator.credentials.create({
      publicKey: credentialCreationOptions
    }).then(opts => {
      const toSend = {
        id: opts.id,
        rawId: btoa(String.fromCharCode.apply(null, new Uint8Array(opts.rawId))),
        response: {
          attestationObject: btoa(String.fromCharCode.apply(null, new Uint8Array(opts.response.attestationObject))),
          clientDataJSON: btoa(String.fromCharCode.apply(null, new Uint8Array(opts.response.clientDataJSON)))
        }
      }
      if(opts.extensions) {
        toSend.extensions = opts.extensions;
      }
      cb(toSend);
    });
  },
}
