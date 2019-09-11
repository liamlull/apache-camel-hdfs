package org.apache.camel.component.hdfs.kerberos;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.*;

public class HdfsKerberosConfigurationFactoryLegacyTest {

    @Test
    public void setupKerberosHdfsConfiguration() throws IOException {
        // given
        Configuration hdfsConfiguration = new Configuration();
        String namedNodes = "node1:1234,node2:5678";
        String kerberosConfigFileLocation = "/some/file/path";
        int replicationFactor = 3;

        // when
        HdfsKerberosConfigurationFactoryLegacy.setupKerberosHdfsConfiguration(hdfsConfiguration, namedNodes, kerberosConfigFileLocation, replicationFactor);

        // then
        assertThat(hdfsConfiguration, notNullValue());

    }

    @Test
    public void loginWithKeytab() throws IOException {
        // given
        Configuration hdfsConfiguration = null;
        String username = null;
        String keyTabFileLocation = null;

        // when
        HdfsKerberosConfigurationFactoryLegacy.loginWithKeytab(hdfsConfiguration, username, keyTabFileLocation);

        // then

    }
}