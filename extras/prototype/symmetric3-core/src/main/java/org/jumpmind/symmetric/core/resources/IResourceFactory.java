package org.jumpmind.symmetric.core.resources;

import java.io.InputStream;

import org.xmlpull.v1.XmlPullParser;

public interface IResourceFactory {

    public InputStream getRawResource(String name);

    public XmlPullParser getXml(String name);

}
