import javax.net.ssl.HostnameVerifier
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate

/**
 * Copied from https://gist.github.com/milhomem/cd322bf3d0599ceb76fe
 */
class SSLContextBuilder {

	SSLContext buildSSLContext(keyStorePath, keyStorePassword, serverCertificatePath) {
		KeyStore keyStore = KeyStore.getInstance("PKCS12")
		FileInputStream clientCertificateContent = new FileInputStream(keyStorePath)
		keyStore.load(clientCertificateContent, keyStorePassword.toCharArray())

		KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
		keyManagerFactory.init(keyStore, keyStorePassword.toCharArray())

		FileInputStream myTrustedCAFileContent = new FileInputStream(serverCertificatePath)
		CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509")
		X509Certificate myCAPublicKey = (X509Certificate) certificateFactory.generateCertificate(myTrustedCAFileContent)

		KeyStore trustedStore = KeyStore.getInstance(KeyStore.getDefaultType())
		trustedStore.load(null)
		trustedStore.setCertificateEntry(myCAPublicKey.getSubjectX500Principal().getName(), myCAPublicKey)
		TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
		trustManagerFactory.init(trustedStore)

		SSLContext sslContext = SSLContext.getInstance("TLS")
		sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null)
		return sslContext
	}

	HostnameVerifier buildNullHostnameVerifier() {
		return [verify: { hostname, session -> true }] as HostnameVerifier
	}
}