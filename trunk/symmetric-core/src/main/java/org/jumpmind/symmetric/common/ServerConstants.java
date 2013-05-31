package org.jumpmind.symmetric.common;

/**
 * These are properties that are server wide.  They can be accessed via the parameter service or via System properties.
 */
public class ServerConstants {
    
    public final static String HOST_BIND_NAME = "host.bind.name";

    public final static String HTTP_ENABLE = "http.enable";
    public final static String HTTP_PORT = "http.port";

    public final static String HTTPS_ENABLE = "https.enable";
    public final static String HTTPS_PORT = "https.port";
    
    public final static String HTTPS_VERIFIED_SERVERS = "https.verified.server.names";
    public final static String HTTPS_ALLOW_SELF_SIGNED_CERTS = "https.allow.self.signed.certs";    
    
    public final static String JMX_HTTP_ENABLE = "jmx.http.enable";
    public final static String JMX_HTTP_PORT = "jmx.http.port";

}