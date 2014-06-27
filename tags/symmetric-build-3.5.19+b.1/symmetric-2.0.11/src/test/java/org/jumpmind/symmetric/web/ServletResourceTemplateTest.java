package org.jumpmind.symmetric.web;

import org.junit.Assert;
import org.junit.Test;

public class ServletResourceTemplateTest {

    @Test
    public void testUriMatch() {
        ServletResourceTemplate template = new ServletResourceTemplate();
        template.setUriPatterns(new String[] { "/pull/*", "/push/*" });
        Assert.assertTrue(template.matchesUriPatterns("/pull"));
        Assert.assertTrue(template.matchesUriPatterns("/push"));
        Assert.assertTrue(template.matchesUriPatterns("/pull/laaaa/teeee/daaa"));
    }
}
