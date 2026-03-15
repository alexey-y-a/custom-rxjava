package ru.customrx;

import org.junit.jupiter.api.Test;
import ru.customrx.core.Disposable;
import ru.customrx.core.Observable;
import ru.customrx.core.Observer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class DisposableTest {

    @Test
    void testDisposeStopsReceivingEvents() {
        List<String> receivedItems = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        Disposable disposable = Observable.<String>create(emitter -> {
            for (int i = 1; i <= 5; i++) {
                if (Thread.currentThread().isInterrupted()) {
                    return;
                }
                emitter.onNext("Item " + i);
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            emitter.onComplete();
        }).subscribe(new Observer<String>() {
            @Override
            public void onNext(String item) {
                receivedItems.add(item);
            }

            @Override
            public void onError(Throwable t) {
                fail("onError not expected");
            }

            @Override
            public void onComplete() {
                completed.set(true);
            }
        });

        try { Thread.sleep(120); } catch (InterruptedException e) {}

        disposable.dispose();

        try { Thread.sleep(200); } catch (InterruptedException e) {}

        assertFalse(completed.get(), "onComplete не должен вызываться после dispose");
        assertTrue(receivedItems.size() >= 1, "Должны быть получены первые элементы");

        int sizeAfterDispose = receivedItems.size();

        try { Thread.sleep(200); } catch (InterruptedException e) {}

        assertEquals(sizeAfterDispose, receivedItems.size(),
                "После dispose не должно быть новых элементов");
    }
}