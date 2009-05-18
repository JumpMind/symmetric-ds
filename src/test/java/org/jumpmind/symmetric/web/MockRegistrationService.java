/**
 * 
 */
package org.jumpmind.symmetric.web;

final class MockRegistrationService extends org.jumpmind.symmetric.service.mock.MockRegistrationService {
    public boolean isAutoRegistration() {
        return true;
    }
}