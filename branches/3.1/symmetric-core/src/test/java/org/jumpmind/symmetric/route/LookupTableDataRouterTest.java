package org.jumpmind.symmetric.route;

import junit.framework.Assert;

import org.jumpmind.symmetric.SyntaxParsingException;
import org.junit.Test;

public class LookupTableDataRouterTest {

    @Test
    public void testValidExpression() {
        LookupTableDataRouter router = new LookupTableDataRouter();
        
        boolean valid = true;
        try {
            router.parse("LOOKUP_TABLE=STORE\r\n" +
                		 "KEY_COLUMN=BRAND_ID\r\n" +
                		 "LOOKUP_KEY_COLUMN=BRAND_ID\r\n" +
                		 "EXTERNAL_ID_COLUMN=STORE_ID");
        } catch(SyntaxParsingException ex) {
            valid = false;
        }
        
        Assert.assertEquals(true, valid);
    }

    @Test
    public void testExpressionWithoutNewLines() {
        LookupTableDataRouter router = new LookupTableDataRouter();
        
        boolean valid = true;
        try {
            router.parse("LOOKUP_TABLE=STORE KEY_COLUMN=BRAND_ID " +
                         "LOOKUP_KEY_COLUMN=BRAND_ID  EXTERNAL_ID_COLUMN=STORE_ID");
        } catch(SyntaxParsingException ex) {
            valid = false;
        }
        
        Assert.assertEquals(false, valid);
    }
    
    @Test
    public void testMissingEqualSign() {
        LookupTableDataRouter router = new LookupTableDataRouter();
        
        boolean valid = true;
        try {
            router.parse("LOOKUP_TABLE=STORE\r\n" +
                         "KEY_COLUMNBRAND_ID\r\n" +         // <-- Missing Equal
                         "LOOKUP_KEY_COLUMN=BRAND_ID\r\n" +
                         "EXTERNAL_ID_COLUMN=STORE_ID");
        } catch(SyntaxParsingException ex) {
            valid = false;
        }
        
        Assert.assertEquals(false, valid);
    }
    
    @Test
    public void testBadKey() {
        LookupTableDataRouter router = new LookupTableDataRouter();
        
        boolean valid = true;
        try {
            router.parse("LOOKUP_TAB=STORE\r\n" +       // <-- Should be LOOKUP_TABLE
                         "KEY_COLUMN=BRAND_ID\r\n" +
                         "LOOKUP_KEY_COLUMN=BRAND_ID\r\n" +
                         "EXTERNAL_ID_COLUMN=STORE_ID");
        } catch(SyntaxParsingException ex) {
            valid = false;
        }
        
        Assert.assertEquals(false, valid);
    }
    
    @Test
    public void testDoubleLine() {
        LookupTableDataRouter router = new LookupTableDataRouter();
        
        boolean valid = true;
        try {
            router.parse("LOOKUP_TABLE=STORE\r\n" +
                         "LOOKUP_TABLE=STORE\r\n" +     // <-- Duplicate
                         "KEY_COLUMN=BRAND_ID\r\n" +
                         "LOOKUP_KEY_COLUMN=BRAND_ID\r\n" +
                         "EXTERNAL_ID_COLUMN=STORE_ID");
        } catch(SyntaxParsingException ex) {
            valid = false;
        }
        
        Assert.assertEquals(false, valid);
    }
    
    @Test
    public void testMissingLine() {
        LookupTableDataRouter router = new LookupTableDataRouter();
        
        boolean valid = true;
        try {
            router.parse("LOOKUP_TABLE=STORE\r\n" +
                         "LOOKUP_KEY_COLUMN=BRAND_ID\r\n" +
                         "EXTERNAL_ID_COLUMN=STORE_ID");
        } catch(SyntaxParsingException ex) {
            valid = false;
        }
        
        Assert.assertEquals(false, valid);
    }
}
