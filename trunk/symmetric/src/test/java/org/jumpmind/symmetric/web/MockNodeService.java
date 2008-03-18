/**
 * Copyright (C) 2005 Big Lots Inc.
 */

package org.jumpmind.symmetric.web;

public class MockNodeService extends org.jumpmind.symmetric.service.mock.MockNodeService
{
    static final String GOOD_SECURITY_TOKEN = "1";

    static final String GOOD_NODE_ID = "1";

    @Override
    public boolean isNodeAuthorized(String nodeId, String password)
    {
        return GOOD_NODE_ID.equals(nodeId) && GOOD_SECURITY_TOKEN.equals(password);
    }
}