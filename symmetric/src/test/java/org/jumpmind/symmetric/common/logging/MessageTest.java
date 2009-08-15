package org.jumpmind.symmetric.common.logging;

import java.util.Locale;

import org.jumpmind.symmetric.common.Message;
import org.junit.Assert;
import org.junit.Test;

public class MessageTest {

    @Test
    public void getBasicMessages() {
        Message.setLocale(new Locale("en_US"));
        long l = 4l;
        String s = Message.get("BatchCompleting");
        Assert.assertEquals("Completing batch null", s);
        s = Message.get("BatchCompleting", l);
        Assert.assertEquals("Completing batch 4", s);

    }

    @Test
    public void getSpanishMessages() {
        Message.setLocale(new Locale("es"));
        long l = 4l;
        String s = Message.get("BatchCompleting");
        Assert.assertEquals("Realizaci—n de la hornada null", s);
        s = Message.get("BatchCompleting", l);
        Assert.assertEquals("Realizaci—n de la hornada 4", s);
    }

    @Test
    public void getBadLanguageMessages() {
        Locale.setDefault(new Locale("en_US"));
        Message.setLocale(new Locale("zz"));
        long l = 4l;
        String s = Message.get("BatchCompleting");
        Assert.assertEquals("Completing batch null", s);
        s = Message.get("BatchCompleting", l);
        Assert.assertEquals("Completing batch 4", s);

    }
}
