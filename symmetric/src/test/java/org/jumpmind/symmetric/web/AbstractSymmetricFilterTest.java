package org.jumpmind.symmetric.web;

import java.util.Map;

import org.jumpmind.symmetric.test.AbstractWebTest;

public class AbstractSymmetricFilterTest extends AbstractWebTest {

    protected String method;
    protected String uri;
    protected Map<String, String> parameters;

    public AbstractSymmetricFilterTest(String method, String uri, Map<String, String> parameters) throws Exception {
        this.method = method;
        this.uri = uri;
        this.parameters = parameters;
    }
}
