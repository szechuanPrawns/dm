package edu.znufe.dm.zk;

import lombok.SneakyThrows;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.RetryNTimes;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestConnectivity {


    @SneakyThrows
    @Test
    public void testConnectiviet(){
        int sleepMsBetweenRetries = 100;
        int maxRetries = 3;
        RetryPolicy retryPolicy = new RetryNTimes(
                maxRetries, sleepMsBetweenRetries);

        CuratorFramework client = CuratorFrameworkFactory.newClient(
                "127.0.0.1:2181", retryPolicy
        );

        client.start();

        assertThat(client.checkExists().forPath("/")).isNotNull();
    }

    @SneakyThrows
    @Test
    public void testDistributedLock(){
        int sleepMsBetweenRetries = 100;
        int maxRetries = 3;
        RetryPolicy retryPolicy = new RetryNTimes(
                maxRetries, sleepMsBetweenRetries);

        CuratorFramework client = CuratorFrameworkFactory.newClient(
                "127.0.0.1:2181", retryPolicy
        );
        client.start();

        Runnable r = ()->{
            CuratorFramework clientX = CuratorFrameworkFactory.newClient(
                    "127.0.0.1:2181", retryPolicy
            );
            clientX.start();
            InterProcessSemaphoreMutex sharedLock = new InterProcessSemaphoreMutex(
                    clientX, "/mutex/process/C");

            Random random = new Random();

            try {
                System.out.println("try get "+sharedLock);
                sharedLock.acquire();
                System.out.println("try get "+sharedLock+": done");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            try {
                Thread.sleep(100 * random.nextInt(10));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            try {
                sharedLock.release();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        for (int i = 0; i < 10; i++) {
            new Thread(r).start();
        }
        Thread.currentThread().join();
    }


    @Test
    public void testAtomicLong() throws Exception {
        int sleepMsBetweenRetries = 100;
        int maxRetries = 3;
        RetryPolicy retryPolicy = new RetryNTimes(
                maxRetries, sleepMsBetweenRetries);

        CuratorFramework client = CuratorFrameworkFactory.newClient(
                "127.0.0.1:2181", retryPolicy
        );
        client.start();

        DistributedAtomicLong atomicLong = new DistributedAtomicLong(client, "/atomic/frontend/ssn", null);

        Runnable justIncrement = ()->{

            try {
                AtomicValue<Long> l = atomicLong.increment();
                System.out.println(l.postValue() + "-" + l.preValue());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            try {
                Thread.sleep(100000000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };

        for (int i = 0; i < 10; i++) {
            new Thread(justIncrement).start();
            System.out.println(atomicLong.get().postValue() + "-" + atomicLong.get().preValue());
            Thread.sleep(10000);

        }
        Thread.currentThread().join(100000000);

    }

    @Test
    public void testBarrier() throws Exception {
        int sleepMsBetweenRetries = 100;
        int maxRetries = 3;
        RetryPolicy retryPolicy = new RetryNTimes(
                maxRetries, sleepMsBetweenRetries);

        CuratorFramework client = CuratorFrameworkFactory.newClient(
                "127.0.0.1:2181", retryPolicy
        );
        client.start();

        DistributedBarrier barrier = new DistributedBarrier(client, "/barrier/concile");

        try{
            barrier.setBarrier();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Runnable waitOnBarrier = ()->{
            try {
                barrier.waitOnBarrier();
                System.out.println("终于等到");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        for (int i = 0; i < 10; i++) {
            new Thread(waitOnBarrier).start();
        }
        Thread.sleep(1000);
        barrier.removeBarrier();
        Thread.currentThread().join(1000);
    }
}
