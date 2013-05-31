package org.jumpmind.symmetric;

import org.junit.Assert;
import org.junit.Test;

public class VersionUnitTest {

    @Test
    public void testIsOlderThanVersion() {
        Assert.assertTrue(Version.isOlderThanVersion("1.5.1", "1.6.0"));
        Assert.assertTrue(Version.isOlderThanVersion("1.3.1", "1.6.0"));
        Assert.assertTrue(Version.isOlderThanVersion("1.6.0", "1.6.1"));
        Assert.assertFalse(Version.isOlderThanVersion("1.6.0", "1.6.0"));
        Assert.assertFalse(Version.isOlderThanVersion("1.6.1", "1.6.0"));
        Assert.assertFalse(Version.isOlderThanVersion("2.0.0", "1.6.0"));
    }
    
    @Test
    public void testIsOlderVersion() {
        // test/resources pom.properties contains 1.6.0
        Assert.assertTrue(Version.isOlderVersion("1.0.0"));
        Assert.assertTrue(Version.isOlderVersion("1.5.0"));
        Assert.assertTrue(Version.isOlderVersion("1.5.1"));
        Assert.assertTrue(Version.isOlderVersion("1.5.5"));
        Assert.assertTrue(Version.isOlderVersion("1.6.0"));
        Assert.assertTrue(Version.isOlderVersion("1.6.1"));
        Assert.assertFalse(Version.isOlderVersion("3.6.1"));
    }
}