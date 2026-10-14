//! TLS as libpq does it, the same as the Rust block SDK's (sdk/src/postgres.rs), so a
//! profile connects the same way from blocks and from stream sinks: `disable`; `allow` and
//! `prefer` (TLS when offered, unchecked); `require` (unchecked, unless a root certificate
//! is given, which makes it `verify-ca`); `verify-ca` (the chain); `verify-full` (and the
//! name).

use std::sync::Arc;

use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{CertificateError, DigitallySignedStruct, Error, RootCertStore, SignatureScheme};

/// What of the server's certificate is checked.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Check {
    None,
    Chain,
    Full,
}

pub fn connector(
    check: Check,
    root_file: Option<&str>,
) -> Result<tokio_postgres_rustls::MakeRustlsConnect, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    if let Some(path) = root_file {
        let certificates = CertificateDer::pem_file_iter(path)
            .map_err(|e| format!("could not read the root certificates in {path}: {e}"))?;
        for certificate in certificates {
            let certificate =
                certificate.map_err(|e| format!("{path} is not a PEM certificate: {e}"))?;
            roots
                .add(certificate)
                .map_err(|e| format!("{path} holds a certificate rustls can't use: {e}"))?;
        }
    }
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?;
    let config = if check == Check::Full {
        builder.with_root_certificates(roots).with_no_client_auth()
    } else {
        let webpki = WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider.clone())
            .build()
            .map_err(|e| e.to_string())?;
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(Verifier {
                check,
                webpki,
                provider,
            }))
            .with_no_client_auth()
    };
    Ok(tokio_postgres_rustls::MakeRustlsConnect::new(config))
}

/// `require` and `verify-ca`: the handshake's signatures are always verified, the
/// certificate's chain only for `verify-ca`, its name never.
#[derive(Debug)]
struct Verifier {
    check: Check,
    webpki: Arc<WebPkiServerVerifier>,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for Verifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        if self.check == Check::None {
            return Ok(ServerCertVerified::assertion());
        }
        match self.webpki.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Err(Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_tls12_signature(
            message,
            certificate,
            signature,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_tls13_signature(
            message,
            certificate,
            signature,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}
