package org.jumpmind.symmetric.transport;

import java.net.URI;

import junit.framework.Assert;

import org.junit.Test;

public class AbstractTransportManagerTest {

    @Test
    public void testChooseURL() {
        AbstractTransportManager tm = getMockTransportManager();
        Assert.assertEquals("test",tm.resolveURL("ext://me/", null));
    }
    
    @Test
    public void testChooseBadURL() {
        AbstractTransportManager tm = getMockTransportManager();
        String notFound = "ext://notfound/";
        Assert.assertEquals(notFound,tm.resolveURL(notFound, null));
    }
    
    protected AbstractTransportManager getMockTransportManager() {
        AbstractTransportManager tm = new AbstractTransportManager() {};
       
        tm.addExtensionSyncUrlHandler("me", new ISyncUrlExtension() {
            public String resolveUrl(URI url) {
                return "test";
            }
        });
        return tm;
    }
}