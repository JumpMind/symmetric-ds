package org.jumpmind.symmetric.service.impl;

import java.io.File;
import java.util.List;

import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeCommunication;
import org.jumpmind.symmetric.model.RemoteNodeStatus;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.ILoadExtractService;
import org.jumpmind.symmetric.service.INodeCommunicationService.INodeCommunicationExecutor;
import org.jumpmind.symmetric.service.IParameterService;

public class InitialLoadExtractorService extends AbstractService implements ILoadExtractService,
        INodeCommunicationExecutor {

    IDataService dataService;

    IDataExtractorService dataExtractorService;

    public InitialLoadExtractorService(IParameterService parameterService,
            ISymmetricDialect symmetricDialect, IDataExtractorService dataExtractorService,
            IDataService dataService) {
        super(parameterService, symmetricDialect);
        this.dataService = dataService;
        this.dataExtractorService = dataExtractorService;
    }

    public void queueLoadExtracts() {
        /*
         * Called by the load extract job to queue up load requests. The logic
         * should be similar to RouterService.insertInitialLoadEvents to figure
         * out which nodes need a load extracted.
         * 
         * Look at FileSyncService.queueJob to see how to queue up work using
         * the NodeCommunicationService (there is probably some duplicate code
         * across PullService, PushService and FileSyncService)
         */
    }

    public void execute(NodeCommunication nodeCommunication, RemoteNodeStatus status) {
        /*
         * This is where the initial load events are inserted into sym_data.
         * 
         * DataService.insertReloadEvents currently inserts reload events. We
         * would probably deprecate the method and move the logic here.
         * 
         * As we iterate over the list of TriggerRouters that need to be
         * extracted we should check to see if an extract has already occurred
         * (in the case of a restart midway through an initial load extraction).
         * Each table that was extracted should have a done file.
         * 
         * Loop: 
         * 
         * Use ILoadExtract to extract files. This should be configured in
         * sym_trigger. We can add a column names load_extract_type
         * 
         * --- The default implementation will create a DataProcessor that reads
         * its data using ExtractDataReader and SelectFromTableSource and writes
         * its data using a new writer named MultipleFileExtractWriter.
         * MultipleFileExtractWriter will use IStagingManager to get
         * IStagedResources (see FileSyncZipDataWriter for an example of using a
         * IStagedResource that always writes to a file). After the
         * DataProcessor has run the MultipleFileExtractWriter will have a
         * getFiles() method to get a handle to the files that were written. ---
         * 
         * Insert each set of sym_data events after the table is
         * extracted and write the done file.
         * 
         * End Loop
         * 
         * Set sym_node_security.initial_load_enabled=0, initial_load_time=current_timestamp
         */
    }

    interface ILoadExtract extends IExtensionPoint {

        public List<File> extract(Node targetNode, TriggerRouter triggerRouter,
                LoadExtractFileHandleFactory fileHandleFactory);

    }

    class LoadExtractFileHandleFactory {
        public File getFileName(int loadId, Node targetNode, TriggerRouter triggerRouter,
                int fileNumber) {
            return null;
        }
    }

}
