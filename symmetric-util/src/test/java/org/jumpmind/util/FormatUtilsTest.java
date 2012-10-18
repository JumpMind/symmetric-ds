package org.jumpmind.util;

import java.util.HashMap;
import java.util.Map;

import org.jumpmind.util.FormatUtils;
import org.junit.Assert;
import org.junit.Test;

public class FormatUtilsTest {

    @Test
    public void testReplaceTokens() {
        Assert.assertEquals("test", FormatUtils.replaceTokens("test", null, true));
        Assert.assertEquals("test", FormatUtils.replaceTokens("test", new HashMap<String, String>(), true));
        Map<String, String> params = new HashMap<String, String>();
        params.put("test", "1");
        Assert.assertEquals("test1", FormatUtils.replaceTokens("test$(test)", params, true));
        Assert.assertEquals("test0001", FormatUtils.replaceTokens("test$(test|%04d)", params, true));
    }
    
    @Test
    public void testReplaceCurrentTimestamp() {
        String beforeSql = "insert into sym_node values ('00000', 'test-root-group', '00000', 1, null, null, '2.0', null, null, current_timestamp, null, 0, 0, '00000', 'engine')";
        String afterSql = "insert into sym_node values ('00000', 'test-root-group', '00000', 1, null, null, '2.0', null, null, XXXX, null, 0, 0, '00000', 'engine')";
        Map<String,String> replacementTokens = new HashMap<String, String>();
        replacementTokens.put("current_timestamp", "XXXX");
        Assert.assertEquals(afterSql, FormatUtils.replaceTokens(beforeSql, replacementTokens, false));
        
    }
    
    @Test
    public void testIsWildcardMatch() {
        Assert.assertTrue(FormatUtils.isWildCardMatch("TEST_1", "TEST_*"));
        Assert.assertTrue(FormatUtils.isWildCardMatch("TEST_2", "TEST_*"));
        Assert.assertTrue(FormatUtils.isWildCardMatch("TEST_TEST_TEST", "TEST_*"));
        Assert.assertFalse(FormatUtils.isWildCardMatch("NOT_A_MATCH", "TEST_*"));
        Assert.assertFalse(FormatUtils.isWildCardMatch("NOT_A_MATCH_TEST_1", "TEST_*"));
        Assert.assertTrue(FormatUtils.isWildCardMatch("NOT_A_MATCH_TEST_1", "*TEST*"));
        Assert.assertFalse(FormatUtils.isWildCardMatch("B_A", "*A*B"));
        Assert.assertTrue(FormatUtils.isWildCardMatch("A_B", "*A*B"));
    }
}
