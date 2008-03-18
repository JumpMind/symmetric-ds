/**
 * Copyright (C) 2005 Big Lots Inc.
 */

package org.jumpmind.symmetric.web;

public class MockRegistrationService extends org.jumpmind.symmetric.service.mock.MockRegistrationService
{
    public boolean isAutoRegistration()
    {
        return true;
    }
}