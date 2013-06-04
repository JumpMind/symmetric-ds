package org.jumpmind.security;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStore.Entry;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.Date;

import javax.security.auth.x500.X500Principal;

import org.bouncycastle.jce.PKCS10CertificationRequest;
import org.bouncycastle.x509.X509V1CertificateGenerator;
import org.jumpmind.util.AppUtils;

public class BouncyCastleSecurityService extends SecurityService {

    public KeyPair generateRSAKeyPair() throws Exception {
        KeyPairGenerator kpGen = KeyPairGenerator.getInstance("RSA", "BC");
        kpGen.initialize(1024, new SecureRandom());
        return kpGen.generateKeyPair();
    }

    public X509Certificate generateV1Certificate(String host, KeyPair pair) throws Exception {

        host = host == null ? AppUtils.getHostName() : host;
        String certString = String.format(
                "CN=%s, OU=SymmetricDS, O=JumpMind, L=Unknown, ST=Unknown, C=Unknown", host);
        log.info("Installing a default SSL certificate: {}", certString);

        X509V1CertificateGenerator certGen = new X509V1CertificateGenerator();

        certGen.setSerialNumber(BigInteger.valueOf(System.currentTimeMillis()));
        certGen.setIssuerDN(new X500Principal(certString));
        certGen.setNotBefore(new Date(System.currentTimeMillis() - 86400000));
        certGen.setNotAfter(new Date(System.currentTimeMillis() + 788400000000l));
        certGen.setSubjectDN(new X500Principal(certString));
        certGen.setPublicKey(pair.getPublic());
        certGen.setSignatureAlgorithm("SHA256WithRSAEncryption");

        return certGen.generate(pair.getPrivate(), "BC");
    }

    public PKCS10CertificationRequest generateRequest(KeyPair pair) throws Exception {
        return new PKCS10CertificationRequest("SHA256withRSA", new X500Principal(
                "CN=Requested Test Certificate"), pair.getPublic(), null, pair.getPrivate());
    }

    @Override
    public void installDefaultSslCert(String host) {
        synchronized (BouncyCastleSecurityService.class) {
            Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());

            try {
                KeyStore keyStore = getKeyStore(getKeyStorePassword());
                KeyStore.ProtectionParameter param = new KeyStore.PasswordProtection(
                        getKeyStorePassword().toCharArray());
                String alias = System.getProperty(SecurityConstants.SYSPROP_KEYSTORE_CERT_ALIAS,
                        SecurityConstants.ALIAS_SYM_PRIVATE_KEY);
                Entry entry = keyStore.getEntry(alias, param);
                if (entry == null) {
                    KeyPair pair = generateRSAKeyPair();
                    X509Certificate cert = generateV1Certificate(host, pair);

                    X509Certificate[] serverChain = new X509Certificate[] { cert };

                    keyStore.setEntry(alias, new KeyStore.PrivateKeyEntry(pair.getPrivate(),
                            serverChain), param);

                    saveKeyStore(keyStore, getKeyStorePassword());
                }
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }

    }

}
