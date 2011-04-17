package org.jumpmind.symmetric.core.db;

import org.jumpmind.symmetric.core.model.Table;

abstract public class AbstractPlatform implements IPlatform {

    private PlatformInfo platformInfo = new PlatformInfo();

    protected String defaultSchema;

    protected String defaultCatalog;

    public PlatformInfo getPlatformInfo() {
        return platformInfo;
    }

    /**
     * Returns the constraint name. This method takes care of length limitations
     * imposed by some databases.
     * 
     * @param prefix
     *            The constraint prefix, can be <code>null</code>
     * @param table
     *            The table that the constraint belongs to
     * @param secondPart
     *            The second name part, e.g. the name of the constraint column
     * @param suffix
     *            The constraint suffix, e.g. a counter (can be
     *            <code>null</code>)
     * @return The constraint name
     */
    public String getConstraintName(String prefix, Table table, String secondPart, String suffix) {
        StringBuffer result = new StringBuffer();

        if (prefix != null) {
            result.append(prefix);
            result.append("_");
        }
        result.append(table.getName());
        result.append("_");
        result.append(secondPart);
        if (suffix != null) {
            result.append("_");
            result.append(suffix);
        }
        return shortenName(result.toString(), getPlatformInfo().getMaxConstraintNameLength());
    }

    /**
     * Generates a version of the name that has at most the specified length.
     * 
     * @param name
     *            The original name
     * @param desiredLength
     *            The desired maximum length
     * @return The shortened version
     */
    public String shortenName(String name, int desiredLength) {
        // TODO: Find an algorithm that generates unique names
        int originalLength = name.length();

        if ((desiredLength <= 0) || (originalLength <= desiredLength)) {
            return name;
        }

        int delta = originalLength - desiredLength;
        int startCut = desiredLength / 2;

        StringBuffer result = new StringBuffer();

        result.append(name.substring(0, startCut));
        if (((startCut == 0) || (name.charAt(startCut - 1) != '_'))
                && ((startCut + delta + 1 == originalLength) || (name.charAt(startCut + delta + 1) != '_'))) {
            // just to make sure that there isn't already a '_' right before or
            // right after the cutting place (which would look odd with an
            // additional one)
            result.append("_");
        }
        result.append(name.substring(startCut + delta + 1, originalLength));
        return result.toString();
    }

    public String getDefaultCatalog() {
        return defaultCatalog;
    }

    public String getDefaultSchema() {
        return defaultSchema;
    }

    public void setDefaultCatalog(String defaultCatalog) {
        this.defaultCatalog = defaultCatalog;
    }

    public void setDefaultSchema(String defaultSchema) {
        this.defaultSchema = defaultSchema;
    }
    
}
