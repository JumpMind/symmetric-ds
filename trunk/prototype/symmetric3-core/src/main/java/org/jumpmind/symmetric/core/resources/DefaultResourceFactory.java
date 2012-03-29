package org.jumpmind.symmetric.core.resources;

import java.io.InputStream;

import org.jumpmind.symmetric.core.common.IoException;
import org.xmlpull.mxp1.MXParser;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

public class DefaultResourceFactory implements IResourceFactory {

    public InputStream getRawResource(String name) {
        return getClass().getResourceAsStream(name);
    }

    public XmlPullParser getXml(String name) {
        try {
            MXParser xpp = new MXParser();
            xpp.setInput(getRawResource(name), "UTF-8");
            return xpp;
        } catch (XmlPullParserException e) {
            throw new IoException(e);
        }

    }
}
