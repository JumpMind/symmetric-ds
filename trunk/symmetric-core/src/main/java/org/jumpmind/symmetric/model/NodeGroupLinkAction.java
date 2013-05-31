package org.jumpmind.symmetric.model;

/**
 * Identifies the action to take when the event watcher sees events in the event
 * table.
 */
public enum NodeGroupLinkAction {
    
    P("pushes to"), W("waits for pull from"), R("only routes to");
    
    private String description;
    
    NodeGroupLinkAction (String desc) {
        this.description = desc;
    }

    public static NodeGroupLinkAction fromCode(String code) {
        if (code != null && code.length() > 0) {
            if (P.name().equals(code)) {
                return P;
            } else if (W.name().equals(code)) {
                return W;
            } else if (R.name().equals(code)) {
                return R;
            }
        }
        return null;
    }
    
    @Override
    public String toString() {     
        return description;
    }

}