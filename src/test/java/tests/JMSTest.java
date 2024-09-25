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
public class JMSTest {

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
        senderThreads.submit(() -> MQQueue.SINGLE_THREAD_CONSUMER_AND_PRODUCER_ONE_MESSAGE.sendMessage(message));

        Callable<String> callable = MQQueue.SINGLE_THREAD_CONSUMER_AND_PRODUCER_ONE_MESSAGE::receiveMessage;
        senderThreads.submit(callable);
        String retrievedMessages = callable.call();

        Assert.assertEquals(message, retrievedMessages);
        Assert.assertNotSame(message, retrievedMessages);

    }

    @Test
    public void testProduceMultipleMessagesConsumesMultipleMessagesSingleThread() throws Exception {

        senderThreads = Executors.newSingleThreadScheduledExecutor();
        receiverThreads = Executors.newSingleThreadScheduledExecutor();

        int size = 1000;

        // send all
        List<String> messages = buildMessages(size);
        sendMessages(messages, senderThreads, MQQueue.SINGLE_THREAD_CONSUMER_AND_PRODUCER_MANY_MESSAGES);

        // receive all
        List<String> retrievedMessages = retrieveMessages(size, receiverThreads, MQQueue.SINGLE_THREAD_CONSUMER_AND_PRODUCER_MANY_MESSAGES);

        // verify
        verifyMessages(messages, retrievedMessages);

    }

    @Test
    public void testProduceMultipleMessagesConsumesMultipleMessagesOnMultipleThreads() throws Exception {

        senderThreads = Executors.newFixedThreadPool(10);
        receiverThreads = Executors.newFixedThreadPool(10);

        int size = 1000;

        // send all
        List<String> messages = buildMessages(size);
        sendMessages(messages, senderThreads, MQQueue.MANY_THREADS_CONSUMER_AND_PRODUCER);

        // receive all
        List<String> retrievedMessages = retrieveMessages(size, receiverThreads, MQQueue.MANY_THREADS_CONSUMER_AND_PRODUCER);

        // verify
        verifyMessages(messages, retrievedMessages);

    }

    @Test
    public void testProduceMultipleMessagesConsumesMultipleMessagesOnMultipleThreadsSingleThreadPool() throws Exception {

        mixedThreads = Executors.newFixedThreadPool(10);

        int size = 1000;

        // send all
        List<String> messages = buildMessages(size);
        sendMessages(messages, mixedThreads, MQQueue.MIXED);

        // receive all
        List<String> retrievedMessages = retrieveMessages(size, mixedThreads, MQQueue.MIXED);

        // verify
        verifyMessages(messages, retrievedMessages);

    }

    private static List<String> buildMessages(int size) {
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            messages.add("message" + String.format("%03d", i));
        }
        return messages;
    }

    private static void verifyMessages(List<String> messages, List<String> retrievedMessages) {

        // TODO why fail on read all messages ?
        Assert.assertEquals(messages.size(), retrievedMessages.size());

        try {
            Collections.sort(retrievedMessages);
        } catch (Exception e) {
            System.out.println(retrievedMessages);
        }

        System.out.println(retrievedMessages);
        System.out.println("received " + new HashSet<>(retrievedMessages).size() + " expected " + messages.size());
        Assert.assertEquals(messages, retrievedMessages);
    }

    private void sendMessages(List<String> messages, ExecutorService executorService, MQQueue queue) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(messages.size());
        for (String message : messages) {
            executorService.submit(() -> {
                queue.sendMessage(message);
                latch.countDown();
            });
        }
        latch.await();
    }

    private List<String> retrieveMessages(int size, ExecutorService executorService, MQQueue queue) throws InterruptedException {
        List<String> retrievedMessages = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(size);
        for (int i = 0; i < size; i++) {
            executorService.submit(() -> {
                retrievedMessages.add(queue.receiveMessage());
                latch.countDown();
            });
        }
        latch.await();
        return retrievedMessages;
    }


}
