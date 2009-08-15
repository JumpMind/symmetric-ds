package org.jumpmind.symmetric.common.logging;

import java.util.Locale;

import org.junit.Assert;
import org.junit.Test;

public class LogTest {

    private ILog log = LogFactory.getLog(this.getClass());

    @Test
    public void getBasicMessages() {
        log.setLocale(new Locale("en_US"));
        long l = 4l;
        String s = log.getMessage("BatchCompleting");
        Assert.assertEquals("Completing batch null", s);
        s = log.getMessage("BatchCompleting", l);
        Assert.assertEquals("Completing batch 4", s);

    }

    @Test
    public void getSpanishMessages() {
        log.setLocale(new Locale("es"));
        long l = 4l;
        String s = log.getMessage("BatchCompleting");
        Assert.assertEquals("Realizaci—n de la hornada null", s);
        s = log.getMessage("BatchCompleting", l);
        Assert.assertEquals("Realizaci—n de la hornada 4", s);
    }

    @Test
    public void getBadLanguageMessages() {
        Locale.setDefault(new Locale("en_US"));
        log.setLocale(new Locale("zz"));
        long l = 4l;
        String s = log.getMessage("BatchCompleting");
        Assert.assertEquals("Completing batch null", s);
        s = log.getMessage("BatchCompleting", l);
        Assert.assertEquals("Completing batch 4", s);

    }
}
