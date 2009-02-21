package org.jumpmind.symmetric.test;

import java.io.File;

import javax.annotation.Resource;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.SymmetricWebServer;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/multi-tier-test.xml" })
public class MultiTierTest {

    static final Log logger = LogFactory.getLog(MultiTierTest.class);

    @Resource
    protected SymmetricWebServer homeServer;

    @Resource
    protected SymmetricWebServer region01Server;
    
    @Resource
    protected SymmetricWebServer workstation01Server;

    @BeforeClass
    public static void setup() throws Exception {
        logger.info("Setting up the multi-tiered test");
        FileUtils.deleteDirectory(new File("target/multi-tier"));
    }

    @Test
    public void validateHomeServerStartup() {

    }

    @Test
    public void registerRegion01() {

    }

}
