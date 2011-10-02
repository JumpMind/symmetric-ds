package org.jumpmind.symmetric.core.process.csv;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.jumpmind.symmetric.core.common.IoException;
import org.jumpmind.symmetric.core.process.IDataFilter;

public class FileCsvDataWriter extends AbstractCsvDataWriter {

    FileChannel channel = null;

    protected boolean closeFileOnClose = true;

    public FileCsvDataWriter(File file, IDataFilter... filters) throws IOException {
        super(filters);
        channel = new FileOutputStream(file).getChannel();
    }

    public void close() {
        if (closeFileOnClose) {
            try {
                channel.close();
            } catch (IOException ex) {
                // do nothing. it doesn't really matter.
            }
        }
    }

    @Override
    protected void println(String data) {
        try {
            byte[] bytes = data.getBytes();
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, 0, bytes.length);
            channel.write(byteBuffer);
        } catch (IOException ex) {
            throw new IoException(ex);
        }
    }

    public void setCloseFileOnClose(boolean closeFileOnClose) {
        this.closeFileOnClose = closeFileOnClose;
    }

    public boolean isCloseFileOnClose() {
        return closeFileOnClose;
    }

}
