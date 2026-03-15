package ru.customrx.schedulers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Schedulers {
    private static final Scheduler IO = new ThreadPoolScheduler(Executors.newCachedThreadPool());
    private static final Scheduler COMPUTATION = new ThreadPoolScheduler(
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));
    private static final Scheduler SINGLE = new ThreadPoolScheduler(Executors.newSingleThreadExecutor());

    public static Scheduler io() { return IO; }
    public static Scheduler computation() { return COMPUTATION; }
    public static Scheduler single() { return SINGLE; }

    private static class ThreadPoolScheduler implements Scheduler {
        private final ExecutorService executor;

        ThreadPoolScheduler(ExecutorService executor) {
            this.executor = executor;
        }

        @Override
        public void execute(Runnable task) {
            executor.execute(task);
        }
    }
}
