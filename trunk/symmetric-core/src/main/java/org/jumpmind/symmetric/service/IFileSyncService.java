package org.jumpmind.symmetric.service;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.jumpmind.symmetric.file.DirectorySnapshot;
import org.jumpmind.symmetric.model.FileSnapshot;
import org.jumpmind.symmetric.model.FileTrigger;
import org.jumpmind.symmetric.model.FileTriggerRouter;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

public interface IFileSyncService {

    public void trackChanges(boolean force);

    public List<FileTrigger> getFileTriggers();

    public FileTrigger getFileTrigger(String triggerId);

    public void saveFileTrigger(FileTrigger fileTrigger);

    public void saveFileTriggerRouter(FileTriggerRouter fileTriggerRouter);

    public void deleteFileTriggerRouter(FileTriggerRouter fileTriggerRouter);

    public void deleteFileTrigger(FileTrigger fileTrigger);

    public List<FileTriggerRouter> getFileTriggerRouters(FileTrigger fileTrigger);

    public DirectorySnapshot getDirectorySnapshot(FileTriggerRouter fileTriggerRouter);

    public void save(List<FileSnapshot> changes);

    public List<FileTriggerRouter> getFileTriggerRoutersForCurrentNode();

    public FileTriggerRouter getFileTriggerRouter(String triggerId, String routerId);

    public void loadFilesFromPush(String nodeId, InputStream in, OutputStream out);

    public RemoteNodeStatuses pullFilesFromNodes(boolean force);

    public RemoteNodeStatuses pushFilesToNodes(boolean force);

    public List<OutgoingBatch> sendFiles(ProcessInfo processInfo, Node node, IOutgoingTransport outgoingTransport);

}
