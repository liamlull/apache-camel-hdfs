package org.apache.camel.component.hdfs.kerberos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;

public class HdfsKerberosConfigurationFactoryLegacy {

    /**
     * Class generates the correct HA configuration based on the namedNodes in the configuration:
     * All named nodes have to be qualified: configuration.set("dfs.ha.namenodes.hfdsNamedService","namenode1,namenode2");
     * For each named node the following entries is added
     * configuration.set("dfs.namenode.rpc-address.hfdsNamedService.namenode1", "namenode1:8020");
     * Finally the proxy provider has to be specified:
     * configuration.set("dfs.client.failover.proxy.provider.hfdsNamedService", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
     *
     * @param namedNodes                 - Named nodes in the Hadoop cluster
     * @param kerberosConfigFileLocation - KerberosConfig file
     * @return - Configuration object containing the settings required to connect to the Hadoop cluster
     */
    public static void setupKerberosHdfsConfiguration(Configuration hdfsConfiguration,
                                                      String namedNodes,
                                                      String kerberosConfigFileLocation,
                                                      int replicationFactor) {
        hdfsConfiguration.set("hadoop.security.authentication", "kerberos");

        String nameService = "hfdsNamedService";
        hdfsConfiguration.set(DFSConfigKeys.DFS_REPLICATION_KEY, Integer.toString(replicationFactor));
        hdfsConfiguration.set(DFSConfigKeys.DFS_NAMESERVICES, nameService);
        hdfsConfiguration.set(DFSUtil.addKeySuffixes(DFS_HA_NAMENODES_KEY_PREFIX, nameService), removePort(namedNodes));

        List<String> distinctNodes = Stream.of(namedNodes.split(",")).distinct().collect(Collectors.toList());

        for (String address : distinctNodes) {
            hdfsConfiguration.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY,
                    nameService, removePort(address)), address);
        }

        hdfsConfiguration.set(DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX + "." + nameService,
                ConfiguredFailoverProxyProvider.class.getName());

        hdfsConfiguration.set("fs.defaultFS", "hdfs://" + nameService);
        System.setProperty("java.security.krb5.conf", kerberosConfigFileLocation);
    }

    public static FileSystem loginWithKeytab(Configuration hdfsConfiguration, String username, String keyTabFileLocation) throws IOException {
        UserGroupInformation.setConfiguration(hdfsConfiguration);

        UserGroupInformation.loginUserFromKeytab(username, keyTabFileLocation);
        return FileSystem.get(hdfsConfiguration);
    }

    private static String removePort(String url) {
        return url.replaceAll("\\.|:[0-9]*", "");
    }
}
