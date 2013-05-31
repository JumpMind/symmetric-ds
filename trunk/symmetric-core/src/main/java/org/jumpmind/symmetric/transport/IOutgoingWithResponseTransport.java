
package org.jumpmind.symmetric.transport;

import java.io.BufferedReader;
import java.io.IOException;

public interface IOutgoingWithResponseTransport extends IOutgoingTransport {
    public BufferedReader readResponse() throws IOException;
}