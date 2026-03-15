package ru.customrx;

import org.junit.jupiter.api.Test;
import ru.customrx.core.Observable;
import ru.customrx.core.Observer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ErrorHandlingTest {

    @Test
    void testErrorPropagation() throws InterruptedException {
        RuntimeException error = new RuntimeException("Boom!");
        AtomicReference<Throwable> caught = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<String>create(emitter -> {
            emitter.onError(error);
        }).subscribe(new Observer<String>() {
            @Override public void onNext(String item) {}
            @Override public void onError(Throwable t) {
                caught.set(t);
                latch.countDown();
            }
            @Override public void onComplete() {}
        });

        assertTrue(latch.await(1, TimeUnit.SECONDS), "Should complete");
        assertSame(error, caught.get());
    }

    @Test
    void testMapError() throws InterruptedException {
        RuntimeException error = new RuntimeException("Map error");
        AtomicReference<Throwable> caught = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.just(1, 2, 3)
                .map(i -> {
                    if (i == 2) throw error;
                    return i * 2;
                })
                .subscribe(new Observer<Integer>() {
                    @Override public void onNext(Integer item) {}
                    @Override public void onError(Throwable t) {
                        caught.set(t);
                        latch.countDown();
                    }
                    @Override public void onComplete() {}
                });

        assertTrue(latch.await(1, TimeUnit.SECONDS), "Should complete");
        assertSame(error, caught.get());
    }
}