package org.jumpmind.symmetric.web;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;

public class AbstractHandler {

    protected String getNodeId(HttpServletRequest req) {
        String nodeId = StringUtils.trimToNull(req.getParameter(WebConstants.NODE_ID));
        if (StringUtils.isBlank(nodeId)) {
            // if this is a registration request, we won't have a node id to use. 
            nodeId = StringUtils.trimToNull(req.getParameter(WebConstants.EXTERNAL_ID));
        }
        return nodeId;
    }
    
    protected String getChannelId(HttpServletRequest req) {
        String channelId = StringUtils.trimToNull(req.getParameter(WebConstants.CHANNEL_ID));
        if (StringUtils.isBlank(channelId)) {
            channelId = "none";
        }
        return channelId;        
    }
}
