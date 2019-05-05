package tests;

import context.MQQueue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;

/**
 * NOTE : if one test fails messages can get stuck on the queue => these messages must be manually deleted
 */
public class Tests {

    private ExecutorService senderThreads;
    private ExecutorService receiverThreads;
    private ExecutorService mixedThreads;

    @After
    public void tearDown() {
        if (senderThreads != null) {
            senderThreads.shutdown();
        }
        if (receiverThreads != null) {
            receiverThreads.shutdown();
        }
        if (mixedThreads != null) {
            mixedThreads.shutdown();
        }
    }

    @Test
    public void testProduceOneMessageConsumeOneMessage() throws Exception {

        senderThreads = Executors.newSingleThreadScheduledExecutor();

        final String message = "message";
        senderThreads.submit(() -> MQQueue.SINGLE.sendMessage(message));

        Callable<String> callable = MQQueue.SINGLE::receiveMessage;
        senderThreads.submit(callable);
        String retrievedMessages = callable.call();

        Assert.assertEquals(message, retrievedMessages);
        Assert.assertNotSame(message, retrievedMessages);

    }

    @Test
    public void testProduceMultipleMessagesConsumesMultipleMessagesSingleThread() throws Exception {

        senderThreads = Executors.newSingleThreadScheduledExecutor();
        receiverThreads = Executors.newSingleThreadScheduledExecutor();

        int size = 100;

        // send all
        List<String> messages = buildMessages(size);
        {
            CountDownLatch latch = new CountDownLatch(size);
            for (String message : messages) {
                senderThreads.submit(() -> {
                    MQQueue.MANY.sendMessage(message);
                    latch.countDown();
                });
            }
            latch.await();
        }

        // receive all
        List<String> retrievedMessages = new ArrayList<>();
        {
            CountDownLatch latch = new CountDownLatch(size);
            for (int i = 0; i < size; i++) {
                receiverThreads.submit(() -> {
                    retrievedMessages.add(MQQueue.MANY.receiveMessage());
                    latch.countDown();
                });
            }
            latch.await();
        }

        {// verify
            verifyMessages(size, messages, retrievedMessages);
        }

    }

    @Test
    public void testProduceMultipleMessagesConsumesMultipleMessagesOnMultipleThreads() throws Exception {

        senderThreads = Executors.newFixedThreadPool(10);
        receiverThreads = Executors.newFixedThreadPool(10);

        int size = 100;

        // send all
        List<String> messages = buildMessages(size);
        {
            CountDownLatch latch = new CountDownLatch(size);
            for (String message : messages) {
                senderThreads.submit(() -> {
                    MQQueue.MANY_TO_MANY.sendMessage(message);
                    latch.countDown();
                });
            }
            latch.await();
        }

        // receive all
        List<String> retrievedMessages = new ArrayList<>();
        {
            CountDownLatch latch = new CountDownLatch(size);
            for (int i = 0; i < size; i++) {
                receiverThreads.submit(() -> {
                    retrievedMessages.add(MQQueue.MANY_TO_MANY.receiveMessage());
                    latch.countDown();
                });
            }
            latch.await();
        }

        {// verify
            verifyMessages(size, messages, retrievedMessages);
        }

    }


    @Test
    public void testProduceMultipleMessagesConsumesMultipleMessagesOnMultipleThreadsSingleThreadPool() throws Exception {

        mixedThreads = Executors.newFixedThreadPool(10);

        int size = 1000;

        // send all
        List<String> messages = buildMessages(size);
        {
            CountDownLatch latch = new CountDownLatch(size);
            for (String message : messages) {
                mixedThreads.submit(() -> {
                    MQQueue.MIXED.sendMessage(message);
                    latch.countDown();
                });
            }
            latch.await();
        }

        // receive all
        List<String> retrievedMessages = new ArrayList<>();
        {
            CountDownLatch latch = new CountDownLatch(size);
            for (int i = 0; i < size; i++) {
                mixedThreads.submit(() -> {
                    retrievedMessages.add(MQQueue.MIXED.receiveMessage());
                    latch.countDown();
                });
            }
            latch.await();
        }

        {// verify
            verifyMessages(size, messages, retrievedMessages);
        }

    }

    private static List<String> buildMessages(int size) {
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            messages.add("message" + String.format("%03d", i));
        }
        return messages;
    }

    private static void verifyMessages(int size, List<String> messages, List<String> retrievedMessages) {

        // TODO hy fail on read all messages ?
        Assert.assertEquals(messages.size(), retrievedMessages.size());

        try {
            Collections.sort(retrievedMessages);
        } catch (Exception e) {
            System.out.println(retrievedMessages);
        }

        System.out.println(retrievedMessages);
        System.out.println("received " + new HashSet<>(retrievedMessages).size() + " expected " + size);
        Assert.assertEquals(messages, retrievedMessages);
    }

}
