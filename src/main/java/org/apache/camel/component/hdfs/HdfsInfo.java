/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.hdfs;

import org.apache.camel.component.hdfs.kerberos.HdfsKerberosConfigurationFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public final class HdfsInfo {

    private Configuration conf;
    private FileSystem fileSystem;
    private Path path;

    HdfsInfo(String hdfsPath, HdfsConfiguration configuration) throws IOException {
        this.conf = newConfiguration(configuration);
        this.fileSystem = newFileSystem(hdfsPath, configuration);
        this.path = new Path(hdfsPath);
    }

    public Configuration getConf() {
        return conf;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public Path getPath() {
        return path;
    }

    private static Configuration newConfiguration(HdfsConfiguration configuration) throws IOException {
        Configuration hdfsConfiguration = new Configuration();

        if (configuration.isKerberosAuthentication()) {
            List<String> namedNodes = configuration.getKerberosNamedNodeList();
            String kerberosConfigFileLocation = configuration.getKerberosConfigFileLocation();
            int dfsReplicationFactor = configuration.getReplication();
            HdfsKerberosConfigurationFactory.setupKerberosHdfsConfiguration(hdfsConfiguration, namedNodes, kerberosConfigFileLocation, dfsReplicationFactor);
        }

        return hdfsConfiguration;
    }

    /**
     * this will connect to the hadoop hdfs file system, and in case of no connection
     * then the hardcoded timeout in hadoop is 45 x 20 sec = 15 minutes
     */
    private FileSystem newFileSystem(String hdfsPath, HdfsConfiguration configuration) throws IOException {
        if (configuration.isKerberosAuthentication()) {
            String userName = configuration.getKerberosUsername();
            String keytabLocation = configuration.getKerberosKeytabLocation();
            HdfsKerberosConfigurationFactory.loginWithKeytab(conf, userName, keytabLocation);
        }

        return FileSystem.get(URI.create(hdfsPath), conf);
    }

}
