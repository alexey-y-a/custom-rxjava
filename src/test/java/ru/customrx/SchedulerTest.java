package ru.customrx;

import org.junit.jupiter.api.Test;
import ru.customrx.core.Observable;
import ru.customrx.core.Observer;
import ru.customrx.schedulers.Schedulers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class SchedulerTest {

    @Test
    void testSubscribeOn() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> threadName = new AtomicReference<>();

        Observable.<String>just("test")
                .subscribeOn(Schedulers.io())
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
                        threadName.set(Thread.currentThread().getName());
                        latch.countDown();
                    }
                    @Override public void onError(Throwable t) {}
                    @Override public void onComplete() {}
                });

        boolean completed = latch.await(1, TimeUnit.SECONDS);
        assertTrue(completed, "Latch should complete");
        assertNotNull(threadName.get());
        assertFalse(threadName.get().contains("main"));
    }

    @Test
    void testObserveOn() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> emitThread = new AtomicReference<>();
        AtomicReference<String> observeThread = new AtomicReference<>();

        Observable.<String>create(emitter -> {
                    emitThread.set(Thread.currentThread().getName());
                    emitter.onNext("test");
                    emitter.onComplete();
                })
                .observeOn(Schedulers.single())
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
                        observeThread.set(Thread.currentThread().getName());
                        latch.countDown();
                    }
                    @Override public void onError(Throwable t) {}
                    @Override public void onComplete() {}
                });

        boolean completed = latch.await(1, TimeUnit.SECONDS);
        assertTrue(completed, "Latch should complete");
        assertNotNull(emitThread.get());
        assertNotNull(observeThread.get());
        assertNotEquals(emitThread.get(), observeThread.get());
    }
}