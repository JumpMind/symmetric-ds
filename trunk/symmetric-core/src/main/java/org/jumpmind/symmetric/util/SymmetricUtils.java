package org.jumpmind.symmetric.util;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.db.ISymmetricDialect;

final public class SymmetricUtils {
    
    private SymmetricUtils() {
    }
    
    public static String quote(ISymmetricDialect symmetricDialect, String name) {
        String quote = symmetricDialect.getPlatform().getDatabaseInfo().getDelimiterToken();
        if (StringUtils.isNotBlank(quote)) {
            return quote + name + quote;
        } else {
            return name;
        }
    }
    
}
