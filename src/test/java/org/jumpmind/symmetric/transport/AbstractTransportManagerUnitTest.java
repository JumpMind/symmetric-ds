package org.jumpmind.symmetric.transport;

import java.net.URI;

import junit.framework.Assert;

import org.apache.commons.logging.impl.NoOpLog;
import org.junit.Test;

public class AbstractTransportManagerUnitTest {

    @Test
    public void testChooseURL() {
        AbstractTransportManager tm = getMockTransportManager();
        Assert.assertEquals("test",tm.resolveURL("ext://me/"));
    }
    
    @Test
    public void testChooseBadURL() {
        AbstractTransportManager tm = getMockTransportManager();
        String notFound = "ext://notfound/";
        Assert.assertEquals(notFound,tm.resolveURL(notFound));
    }
    
    protected AbstractTransportManager getMockTransportManager() {
        AbstractTransportManager tm = new AbstractTransportManager(null) {};
        tm.logger = new NoOpLog();
        tm.addExtensionSyncUrlHandler("me", new ISyncUrlExtension() {
            public String resolveUrl(URI url) {
                return "test";
            }
            public boolean isAutoRegister() {
                return false;
            }
        });
        return tm;
    }
}
