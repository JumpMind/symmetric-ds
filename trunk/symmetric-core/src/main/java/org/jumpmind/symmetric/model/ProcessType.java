package org.jumpmind.symmetric.model;

public enum ProcessType {
    ANY, EXTRACT_FOR_PUSH, TRANSFER_TO, TRANSFER_FROM, PULL_JOB, LOAD_FROM_PUSH, PULL_HANDLER, REST_PULL_HANLDER, ROUTER_JOB, INSERT_LOAD_EVENTS, GAP_DETECT, ROUTER_READER, MANUAL_LOAD, FILE_SYNC_PULL_JOB, FILE_SYNC_PUSH_JOB, FILE_SYNC_PULL_HANDLER, FILE_SYNC_PUSH_HANDLER, INITIAL_LOAD_EXTRACT_JOB;

    public String toString() {
        switch (this) {
            case ANY:
                return "<Any>";
            case MANUAL_LOAD:
                return "Manual Load";
            case EXTRACT_FOR_PUSH:
                return "Extract For Push";
            case TRANSFER_FROM:
                return "Transfer From";
            case TRANSFER_TO:
                return "Transfer To";
            case PULL_JOB:
                return "Database Pull";
            case LOAD_FROM_PUSH:
                return "Load From Push";
            case PULL_HANDLER:
                return "Extract For Pull";
            case ROUTER_JOB:
                return "Routing";
            case ROUTER_READER:
                return "Routing Reader";
            case GAP_DETECT:
                return "Gap Detection";
            case FILE_SYNC_PULL_JOB:
                return "File Sync Pull";
            case FILE_SYNC_PUSH_JOB:
                return "File Sync Push";
            case FILE_SYNC_PULL_HANDLER:
                return "Service File Sync Pull";
            case FILE_SYNC_PUSH_HANDLER:
                return "Service File Sync Push";
            case REST_PULL_HANLDER:
                return "REST Pull";
            case INSERT_LOAD_EVENTS:
                return "Inserting Load Events";
            case INITIAL_LOAD_EXTRACT_JOB:
                return "Initial Load Extractor";
            default:
                return name();
        }
    }
}