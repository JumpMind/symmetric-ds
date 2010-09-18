/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.jumpmind.symmetric.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.Version;

public class JarBuilder {

    private File baseDir;

    private File[] sourceFiles;

    private File outputFile;

    public JarBuilder(File baseDir, File outputFile, File[] sourceFiles) {
        this.sourceFiles = sourceFiles;
        this.outputFile = outputFile;
        this.baseDir = baseDir;
    }

    public void build() throws IOException {
        this.outputFile.delete();
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, Version.version());
        outputFile.getParentFile().mkdirs();
        JarOutputStream target = new JarOutputStream(new FileOutputStream(outputFile), manifest);
        for (File file : sourceFiles) {
            add(file, target);
        }
        target.close();
    }

    private String massageJarEntryName(File source) {
        String name = source.getPath();
        if (baseDir != null && name.startsWith(baseDir.getPath()) && name.length() > baseDir.getPath().length()) {
            name = name.substring(baseDir.getPath().length()+1);
        }
        return name.replace("\\", "/");
    }

    private void add(File source, JarOutputStream target) throws IOException {
        BufferedInputStream in = null;
        try {
            if (source.isDirectory()) {
                String name = massageJarEntryName(source);
                if (!name.isEmpty()) {
                    if (!name.endsWith("/")) {
                        name += "/";
                    }
                    JarEntry entry = new JarEntry(name);
                    entry.setTime(source.lastModified());
                    target.putNextEntry(entry);
                    target.closeEntry();
                }
                for (File nestedFile : source.listFiles()) {
                    add(nestedFile, target);
                }
            } else {
                JarEntry entry = new JarEntry(massageJarEntryName(source));
                entry.setTime(source.lastModified());
                target.putNextEntry(entry);
                in = new BufferedInputStream(new FileInputStream(source));

                byte[] buffer = new byte[1024];
                while (true) {
                    int count = in.read(buffer);
                    if (count == -1) {
                        break;
                    }
                    target.write(buffer, 0, count);
                }
                target.closeEntry();
            }
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

}
