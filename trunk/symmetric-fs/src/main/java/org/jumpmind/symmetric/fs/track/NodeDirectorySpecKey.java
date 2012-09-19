package org.jumpmind.symmetric.fs.track;

import org.jumpmind.symmetric.fs.config.DirectorySpec;
import org.jumpmind.symmetric.fs.config.Node;

public class NodeDirectorySpecKey {

    protected String key;

    public NodeDirectorySpecKey(Node node, DirectorySpec spec) {
        this.key = String.format(
                "%s_%s_%s",
                node.getNodeId(),
                node.getGroupId(),
                Integer.toHexString(spec.getDirectory()
                        .replace(System.getProperty("file.separator").charAt(0), '_').hashCode()));
    }

    @Override
    public String toString() {
        return key;
    }
}
