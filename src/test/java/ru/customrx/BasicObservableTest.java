package ru.customrx;

import org.junit.jupiter.api.Test;
import ru.customrx.core.Observable;
import ru.customrx.core.Observer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class BasicObservableTest {

    @Test
    void testCreateAndSubscribe() throws InterruptedException {
        List<String> result = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable<String> observable = Observable.create(emitter -> {
            emitter.onNext("Hello");
            emitter.onNext("World");
            emitter.onComplete();
        });

        observable.subscribe(new Observer<String>() {
            @Override
            public void onNext(String item) {
                result.add(item);
            }

            @Override
            public void onError(Throwable t) {
                fail("Should not error");
            }

            @Override
            public void onComplete() {
                result.add("COMPLETE");
                latch.countDown();
            }
        });

        assertTrue(latch.await(1, TimeUnit.SECONDS), "Should complete");
        assertEquals(3, result.size());
        assertEquals("Hello", result.get(0));
        assertEquals("World", result.get(1));
        assertEquals("COMPLETE", result.get(2));
    }

    @Test
    void testJust() throws InterruptedException {
        List<Integer> result = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.just(1, 2, 3)
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                        result.add(item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail("Should not error");
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        assertTrue(latch.await(1, TimeUnit.SECONDS), "Should complete");
        assertEquals(List.of(1, 2, 3), result);
    }
}