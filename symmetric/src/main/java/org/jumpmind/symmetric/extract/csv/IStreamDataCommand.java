package org.jumpmind.symmetric.extract.csv;

import java.io.BufferedWriter;
import java.io.IOException;

import org.jumpmind.symmetric.model.Data;

interface IStreamDataCommand {
    void execute(BufferedWriter out, Data data) throws IOException;
}