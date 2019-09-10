package org.apache.camel.component.hdfs.kerberos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;

public class HdfsKerberosConfigurationFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsKerberosConfigurationFactory.class);

    private static final String KERBEROS_5_SYS_ENV = "java.security.krb5.conf";

    static final String HFDS_NAMED_SERVICE = "hfdsNamedService";

    private static final String AUTHENTICATION_MODE = "hadoop.security.authentication";
    private static final String HFDS_FS = "fs.defaultFS";

    /**
     * This method generates the correct HA configuration (normally read from xml) based on the namedNodes:
     * All named nodes have to be qualified: configuration.set("dfs.ha.namenodes.hfdsNamedService","namenode1,namenode2");
     * For each named node the following entries is added
     * <p>
     * configuration.set("dfs.namenode.rpc-address.hfdsNamedService.namenode1", "lsrv123.linux.rabo.nl:1234");
     * <p>
     * Finally the proxy provider has to be specified:
     * <p>
     * configuration.set("dfs.client.failover.proxy.provider.hfdsNamedService", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
     * <p>
     *
     * @param hdfsConfiguration          - configuration
     * @param namedNodes                 - All named nodes from the hadoop cluster
     * @param kerberosConfigFileLocation - The location of the kerberos config file (on the server)
     * @param replicationFactor          - dfs replication factor
     */
    public static void setupKerberosHdfsConfiguration(Configuration hdfsConfiguration,
                                                      List<String> namedNodes,
                                                      String kerberosConfigFileLocation,
                                                      int replicationFactor) throws IOException {

        checkFileExists(kerberosConfigFileLocation);

        setupHdfsConfiguration(hdfsConfiguration, namedNodes, replicationFactor);

        //This file needs to be on the file system
        String krb5Conf = System.getProperty(KERBEROS_5_SYS_ENV);
        if (krb5Conf == null || !krb5Conf.isEmpty()) {
            System.setProperty(KERBEROS_5_SYS_ENV, kerberosConfigFileLocation);
        } else if (!krb5Conf.equalsIgnoreCase(kerberosConfigFileLocation)) {
            LOGGER.warn("{} was already configured with: {}", KERBEROS_5_SYS_ENV, krb5Conf);
        }
    }

    /**
     * This method can be used for two purposes. First of all it can be used to simply login and get a filesystem.
     * The second option is to use it in combination with the camel-hadoop component.
     * This component by default doesnt support kerberos auth out of the box. In order to connect to a hadoop cluster
     * using Kerberos you need to add your own filesystem to the cache of the FileSystem component. This is done by setting
     * the uri that you use in your camel route as the URI that is used to setup the connection. The URI is used as key when
     * adding it to the cache (default functionality of the static FileSystem.get(URI, Configuration) method).
     * <p>
     * When connecting with camel-hadoop from your route the config is retrieved from cache and not from the default
     * (non kerberos) config.
     *
     * @param hdfsConfiguration  - Configuration class that contains the cluster details
     * @param username           - Principal used to connect to the cluster
     * @param keyTabFileLocation - KeyTab file location (must be on the server)
     * @throws IOException - In case of error
     */
    public static void loginWithKeytab(Configuration hdfsConfiguration, String username, String keyTabFileLocation) throws IOException {
        checkFileExists(keyTabFileLocation);
        //We need to log in otherwise you cannot connect to the filesystem later on
        UserGroupInformation.setConfiguration(hdfsConfiguration);
        UserGroupInformation.loginUserFromKeytab(username, keyTabFileLocation);
    }

    static void setupHdfsConfiguration(Configuration hdfsConfiguration, List<String> namedNodes, int replicationFactor) {
        hdfsConfiguration.set(AUTHENTICATION_MODE, "kerberos");

        final String serviceName = String.format("%s-%s", HFDS_NAMED_SERVICE, hdfsConfiguration.hashCode());
        String namedNodesData = namedNodes.stream().map(HdfsKerberosConfigurationFactory::removePort).collect(Collectors.joining(","));

        hdfsConfiguration.set(DFSConfigKeys.DFS_REPLICATION_KEY, Integer.toString(replicationFactor));
        hdfsConfiguration.set(DFSConfigKeys.DFS_NAMESERVICES, serviceName);
        hdfsConfiguration.set(DFSUtil.addKeySuffixes(DFS_HA_NAMENODES_KEY_PREFIX, serviceName), namedNodesData);

        namedNodes.forEach(namedNode -> hdfsConfiguration.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY, serviceName, removePort(namedNode)), namedNode));

        hdfsConfiguration.set(DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX + "." + serviceName, ConfiguredFailoverProxyProvider.class.getName());

        hdfsConfiguration.set(HFDS_FS, "hdfs://" + serviceName);
    }

    private static void checkFileExists(String fileLocation) throws FileNotFoundException {
        File file = new File(fileLocation);
        if (!file.exists()) {
            throw new FileNotFoundException(file + " not found.");
        }
    }

    private static String removePort(String url) {
        return url.replaceAll("\\.|:[0-9]*", "");
    }
}
