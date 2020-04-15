package com.du.test.dag;

import io.elasticjob.lite.fixture.EmbedTestingServer;
import io.elasticjob.lite.reg.zookeeper.ZookeeperConfiguration;
import io.elasticjob.lite.reg.zookeeper.ZookeeperRegistryCenter;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestBaseRegCenter {

    private static final String NAME_SPACE = "testbasezk";

    private static final ZookeeperConfiguration ZOOKEEPER_CONFIGURATION = new ZookeeperConfiguration(EmbedTestingServer.getConnectionString(), NAME_SPACE);

    public static ZookeeperRegistryCenter zkRegCenter;

    @BeforeClass
    public static void setUp() {
        EmbedTestingServer.start();
        ZOOKEEPER_CONFIGURATION.setDigest("digest:password");
        ZOOKEEPER_CONFIGURATION.setSessionTimeoutMilliseconds(5000);
        ZOOKEEPER_CONFIGURATION.setConnectionTimeoutMilliseconds(5000);
        zkRegCenter = new ZookeeperRegistryCenter(ZOOKEEPER_CONFIGURATION);
        zkRegCenter.init();
    }

    @AfterClass
    public static void tearDown() {
        zkRegCenter.close();
    }
}
