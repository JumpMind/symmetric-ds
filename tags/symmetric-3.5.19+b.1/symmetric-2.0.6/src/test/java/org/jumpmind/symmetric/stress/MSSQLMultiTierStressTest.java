package org.jumpmind.symmetric.stress;

import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(locations = { "classpath:/stress/sql-server-multi-tier-stress-test.xml" })
public class MSSQLMultiTierStressTest extends AbstractMultiTierStressTest {

}
