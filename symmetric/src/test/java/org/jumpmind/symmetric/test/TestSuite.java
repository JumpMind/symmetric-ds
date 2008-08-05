package org.jumpmind.symmetric.test;

import org.jumpmind.symmetric.web.AuthenticationFilterTest;
import org.jumpmind.symmetric.web.InetAddressFilterTest;
import org.jumpmind.symmetric.web.SymmetricForbiddenFilterTest;
import org.jumpmind.symmetric.web.SymmetricRegistrationRequiredTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses( { InetAddressFilterTest.class, AuthenticationFilterTest.class, SymmetricForbiddenFilterTest.class,
        SymmetricRegistrationRequiredTest.class })
public class TestSuite {

}
