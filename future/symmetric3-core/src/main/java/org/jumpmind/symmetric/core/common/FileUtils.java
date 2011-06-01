package org.jumpmind.symmetric.core.common;

import java.io.File;

abstract public class FileUtils {

    /**
     * Delete a file. If file is a directory, delete it and all sub-directories.
     * <p>
     * The difference between File.delete() and this method are:
     * <ul>
     * <li>A directory to be deleted does not have to be empty.</li>
     * <li>You get exceptions when a file or directory cannot be deleted.
     * (java.io.File methods returns a boolean)</li>
     * </ul>
     * 
     * @param file
     *            file or directory to delete, must not be <code>null</code>
     * @throws NullPointerException
     *             if the directory is <code>null</code> @ in case deletion is
     *             unsuccessful
     */
    public static void forceDelete(File file) {
        if (file.isDirectory()) {
            deleteDirectory(file);
        } else {
            if (!file.exists()) {
                throw new IoException("File does not exist: " + file);
            }
            if (!file.delete()) {
                String message = "Unable to delete file: " + file;
                throw new IoException(message);
            }
        }
    }

    /**
     * Recursively delete a directory.
     * 
     * @param directory
     *            directory to delete @ in case deletion is unsuccessful
     */
    public static void deleteDirectory(String directory) {
        deleteDirectory(new File(directory));
    }

    /**
     * Recursively delete a directory.
     * 
     * @param directory
     *            directory to delete @ in case deletion is unsuccessful
     */
    public static void deleteDirectory(File directory) {
        if (!directory.exists()) {
            return;
        }

        cleanDirectory(directory);
        if (!directory.delete()) {
            String message = "Unable to delete directory " + directory + ".";
            throw new IoException(message);
        }
    }

    /**
     * Clean a directory without deleting it.
     * 
     * @param directory
     *            directory to clean @ in case cleaning is unsuccessful
     */
    public static void cleanDirectory(File directory) {
        if (!directory.exists()) {
            String message = directory + " does not exist";
            throw new IllegalArgumentException(message);
        }

        if (!directory.isDirectory()) {
            String message = directory + " is not a directory";
            throw new IllegalArgumentException(message);
        }

        File[] files = directory.listFiles();
        if (files == null) { // null if security restricted
            throw new IoException("Failed to list contents of " + directory);
        }

        IoException exception = null;
        for (int i = 0; i < files.length; i++) {
            File file = files[i];
            try {
                forceDelete(file);
            } catch (IoException ioe) {
                exception = ioe;
            }
        }

        if (null != exception) {
            throw exception;
        }
    }

}
