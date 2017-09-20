package org.jumpmind.symmetric.model;

public enum ProcessType {
    
    ANY, PUSH_JOB_EXTRACT, PUSH_JOB_TRANSFER, PULL_JOB_TRANSFER, PULL_JOB_LOAD, PUSH_HANDLER_TRANSFER, PUSH_HANDLER_LOAD, PULL_HANDLER_TRANSFER, PULL_HANDLER_EXTRACT, REST_PULL_HANLDER, OFFLINE_PUSH, OFFLINE_PULL, ROUTER_JOB, INSERT_LOAD_EVENTS, GAP_DETECT, ROUTER_READER, MANUAL_LOAD, FILE_SYNC_PULL_JOB, FILE_SYNC_PUSH_JOB, FILE_SYNC_PULL_HANDLER, FILE_SYNC_PUSH_HANDLER, FILE_SYNC_TRACKER, INITIAL_LOAD_EXTRACT_JOB, FILE_SYNC_INITIAL_LOAD_EXTRACT_JOB, PULL_CONFIG_JOB;

    @Override
    public String toString() {
        switch (this) {
            case ANY:
                return "<Any>";
            case MANUAL_LOAD:
                return "Manual Load";
            case PUSH_JOB_EXTRACT:
                return "Database Push Extract";
            case PUSH_JOB_TRANSFER:
                return "Database Push Transfer";
            case PULL_JOB_TRANSFER:
                return "Database Pull Transfer";
            case PULL_JOB_LOAD:
                return "Database Pull Load";
            case PULL_CONFIG_JOB:
                return "Config Pull";
            case PUSH_HANDLER_TRANSFER:
                return "Service Database Push Transfer";
            case PULL_HANDLER_TRANSFER:
                return "Service Database Pull Transfer";
            case PUSH_HANDLER_LOAD:
                return "Service Database Push Load";
            case PULL_HANDLER_EXTRACT:
                return "Service Database Pull Extract";
            case OFFLINE_PUSH:
                return "Offline Push";
            case OFFLINE_PULL:
                return "Offline Pull";
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
            case FILE_SYNC_TRACKER:
                return "File Sync Tracker";
            case REST_PULL_HANLDER:
                return "REST Pull";
            case INSERT_LOAD_EVENTS:
                return "Inserting Load Events";
            case INITIAL_LOAD_EXTRACT_JOB:
                return "Initial Load Extractor";
            case FILE_SYNC_INITIAL_LOAD_EXTRACT_JOB:
                return "File Sync Initial Load Extractor";
            default:
                return name();
        }
    }
};