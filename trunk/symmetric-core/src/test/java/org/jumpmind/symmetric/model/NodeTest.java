package org.jumpmind.symmetric.model;

import junit.framework.Assert;

import org.junit.Test;

public class NodeTest {

    @Test
    public void testIsVersionGreaterThan() {
        Node test = new Node();
        test.setSymmetricVersion("1.5.0");
        Assert.assertTrue(test.isVersionGreaterThanOrEqualTo(1,3,0));
        Assert.assertFalse(test.isVersionGreaterThanOrEqualTo(2,0,0));
        Assert.assertFalse(test.isVersionGreaterThanOrEqualTo(2,0,0));
        Assert.assertTrue(test.isVersionGreaterThanOrEqualTo(1,4,9,1));
        Assert.assertTrue(test.isVersionGreaterThanOrEqualTo(1,5,0));        
        Assert.assertFalse(test.isVersionGreaterThanOrEqualTo(1,5,1));
        test.setSymmetricVersion("1.5.0-SNAPSHOT");
        Assert.assertTrue(test.isVersionGreaterThanOrEqualTo(1,3,0));
        Assert.assertFalse(test.isVersionGreaterThanOrEqualTo(2,0,0));
        Assert.assertTrue(test.isVersionGreaterThanOrEqualTo(1,5,0));    
        test.setSymmetricVersion("development");
        Assert.assertTrue(test.isVersionGreaterThanOrEqualTo(1,3,0));
        Assert.assertTrue(test.isVersionGreaterThanOrEqualTo(2,0,0));        
    }
}