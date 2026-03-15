package ru.customrx;

import org.junit.jupiter.api.Test;
import ru.customrx.core.Observable;
import ru.customrx.core.Observer;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class FlatMapTest {

    @Test
    void testFlatMap() throws InterruptedException {
        ConcurrentLinkedQueue<String> result = new ConcurrentLinkedQueue<>();
        AtomicInteger receivedCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        Observable.just(1, 2)
                .flatMap(i -> Observable.just("A" + i, "B" + i))
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
                        result.add(item);
                        receivedCount.incrementAndGet();
                        System.out.println("Received: " + item + " in " + Thread.currentThread().getName());
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail("Should not error: " + t.getMessage());
                    }

                    @Override
                    public void onComplete() {
                        System.out.println("Completed on " + Thread.currentThread().getName());
                        latch.countDown();
                    }
                });

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Observable should complete");

        Thread.sleep(200);

        List<String> resultList = result.stream().toList();

        System.out.println("Final result size: " + resultList.size());
        System.out.println("Final received count: " + receivedCount.get());
        System.out.println("All items: " + resultList);

        assertEquals(4, receivedCount.get(), "Should receive 4 items");
        assertEquals(4, resultList.size(), "Should have 4 items in list");

        assertTrue(resultList.contains("A1"), "Should contain A1");
        assertTrue(resultList.contains("B1"), "Should contain B1");
        assertTrue(resultList.contains("A2"), "Should contain A2");
        assertTrue(resultList.contains("B2"), "Should contain B2");
    }
}