//! Webhook signatures: an HMAC-SHA256 of the request body in a header, as GitHub
//! (`X-Hub-Signature-256: sha256=<hex>`), Stripe-like and most webhook senders do. Checked in
//! constant time.

use ring::hmac;

/// How a webhook's requests are signed.
#[derive(Debug, Clone)]
pub struct SignatureCheck {
    /// The header that carries the signature, such as `X-Hub-Signature-256`.
    pub header: String,
    /// What comes before the hex digest, such as `sha256=`.
    pub prefix: String,
    key: hmac::Key,
}

impl SignatureCheck {
    pub fn new(header: &str, prefix: &str, secret: &[u8]) -> SignatureCheck {
        SignatureCheck {
            header: header.to_string(),
            prefix: prefix.to_string(),
            key: hmac::Key::new(hmac::HMAC_SHA256, secret),
        }
    }

    /// Whether the header's value signs the body.
    pub fn verify(&self, value: Option<&str>, body: &[u8]) -> bool {
        let Some(value) = value else {
            return false;
        };
        let Some(digest) = value.trim().strip_prefix(self.prefix.as_str()) else {
            return false;
        };
        let Some(signature) = decode_hex(digest) else {
            return false;
        };
        hmac::verify(&self.key, body, &signature).is_ok()
    }

    /// The header value for a body, for tests and clients.
    pub fn sign(&self, body: &[u8]) -> String {
        let tag = hmac::sign(&self.key, body);
        let hex: String = tag.as_ref().iter().map(|b| format!("{b:02x}")).collect();
        format!("{}{hex}", self.prefix)
    }
}

fn decode_hex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(text.get(i..i + 2)?, 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_signed_body_verifies_and_anything_else_does_not() {
        let check = SignatureCheck::new("X-Hub-Signature-256", "sha256=", b"secret");
        let body = br#"{"action":"opened"}"#;
        let signature = check.sign(body);

        assert!(check.verify(Some(&signature), body));
        assert!(!check.verify(Some(&signature), b"{}"));
        assert!(!check.verify(None, body));
        assert!(!check.verify(Some("sha256=zz"), body));
        assert!(!check.verify(Some(&signature.replace("sha256=", "sha1=")), body));
        let other = SignatureCheck::new("X-Hub-Signature-256", "sha256=", b"other");
        assert!(!other.verify(Some(&signature), body));
    }
}
