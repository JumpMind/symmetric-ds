package org.jumpmind.symmetric.common.logging;

import java.util.Locale;

import org.junit.Assert;
import org.junit.Test;

public class LogTest {

    private Log logger = LogFactory.getLog(this.getClass());

    @Test
    public void getBasicMessages() {
        logger.setLocale(new Locale("en_US"));
        long l = 4l;
        String s = logger.getMessage("BatchCompleting");
        Assert.assertEquals("Completing batch null", s);
        s = logger.getMessage("BatchCompleting", l);
        Assert.assertEquals("Completing batch 4", s);

    }

    @Test
    public void getSpanishMessages() {
        logger.setLocale(new Locale("es"));
        long l = 4l;
        String s = logger.getMessage("BatchCompleting");
        Assert.assertEquals("Realizaci—n de la hornada null", s);
        s = logger.getMessage("BatchCompleting", l);
        Assert.assertEquals("Realizaci—n de la hornada 4", s);
    }

    @Test
    public void getBadLanguageMessages() {
        Locale.setDefault(new Locale("en_US"));
        logger.setLocale(new Locale("zz"));
        long l = 4l;
        String s = logger.getMessage("BatchCompleting");
        Assert.assertEquals("Completing batch null", s);
        s = logger.getMessage("BatchCompleting", l);
        Assert.assertEquals("Completing batch 4", s);

    }
}
