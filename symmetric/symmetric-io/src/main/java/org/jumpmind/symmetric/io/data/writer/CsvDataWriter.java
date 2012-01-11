package org.jumpmind.symmetric.io.data.writer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.List;

import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.io.data.Batch;

public class CsvDataWriter extends AbstractCsvDataWriter {

    private BufferedWriter writer;

    public CsvDataWriter(Writer writer) {
        this(null, writer);
    }

    public CsvDataWriter(List<ICsvDataWriterListener> listeners, Writer writer) {
        super(listeners);
        if (writer instanceof BufferedWriter) {
            this.writer = (BufferedWriter) writer;
        } else {
            this.writer = new BufferedWriter(writer);
        }
    }

    @Override
    protected void endBatch(Batch batch) {
        try {
            writer.flush();
        } catch (IOException e) {
            throw new IoException(e);
        }
    }

    @Override
    protected void notifyEndBatch(Batch batch, ICsvDataWriterListener listener) {
    }

    @Override
    protected void print(Batch batch, String data) {
        try {
            writer.write(data);
        } catch (IOException e) {
            throw new IoException(e);
        }
    }

}
