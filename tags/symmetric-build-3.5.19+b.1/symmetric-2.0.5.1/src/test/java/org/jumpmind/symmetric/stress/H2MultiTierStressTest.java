package org.jumpmind.symmetric.stress;

import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(locations = { "classpath:/stress/h2-multi-tier-stress-test.xml" })
public class H2MultiTierStressTest extends AbstractMultiTierStressTest {

}
