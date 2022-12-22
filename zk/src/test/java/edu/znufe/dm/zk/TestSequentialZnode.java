package edu.znufe.dm.zk;

import lombok.SneakyThrows;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestSequentialZnode {


    @SneakyThrows
    @Test
    public void testOverflow(){
        int sleepMsBetweenRetries = 100;
        int maxRetries = 3;
        RetryPolicy retryPolicy = new RetryNTimes(
                maxRetries, sleepMsBetweenRetries);

        CuratorFramework client = CuratorFrameworkFactory.newClient(
                "127.0.0.1:2181", retryPolicy
        );

        client.start();

        assertThat(client.checkExists().forPath("/")).isNotNull();

        Runnable createNode = ()->{
            for (long i = 0L; i < 1000000000L; i++) {
                String node = null;
                try {
                    node = client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/head/child", new byte[0]);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                assertThat(node).isGreaterThanOrEqualTo("/head/child0000000001");
            }
        };

        for (int i = 0; i < 7; i++) {
            new Thread(createNode).start();
        }

        Thread.currentThread().join(1000*60*60);

    }


}
