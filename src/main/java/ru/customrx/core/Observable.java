package ru.customrx.core;

import ru.customrx.schedulers.Scheduler;

public abstract class Observable<T> {

    private static final java.util.concurrent.ExecutorService DEFAULT_EXECUTOR =
            java.util.concurrent.Executors.newCachedThreadPool();

    public Disposable subscribe(Observer<T> observer) {
        return subscribeActual(observer);
    }

    protected Disposable subscribeActual(Observer<T> observer) {
        return new DisposableObserver<>(observer);
    }

    public static <T> Observable<T> create(Consumer<Emitter<T>> emitter) {
        return new Observable<T>() {
            @Override
            public Disposable subscribe(Observer<T> observer) {
                BooleanDisposable disposable = new BooleanDisposable();
                Emitter<T> emitterWrapper = new Emitter<>(observer, disposable);

                DEFAULT_EXECUTOR.execute(() -> {
                    try {
                        emitter.accept(emitterWrapper);
                    } catch (Throwable t) {
                        if (!disposable.isDisposed()) {
                            observer.onError(t);
                        }
                    }
                });

                return disposable;
            }
        };
    }

    @SafeVarargs
    public static <T> Observable<T> just(T... items) {
        return create(emitter -> {
            for (T item : items) {
                emitter.onNext(item);
            }
            emitter.onComplete();
        });
    }

    public <R> Observable<R> map(Function<T, R> mapper) {
        return new MapObservable<>(this, mapper);
    }

    public Observable<T> filter(Predicate<T> predicate) {
        return new FilterObservable<>(this, predicate);
    }

    public <R> Observable<R> flatMap(Function<T, Observable<R>> mapper) {
        return new FlatMapObservable<>(this, mapper);
    }

    public Observable<T> subscribeOn(Scheduler scheduler) {
        return new SubscribeOnObservable<>(this, scheduler);
    }

    public Observable<T> observeOn(Scheduler scheduler) {
        return new ObserveOnObservable<>(this, scheduler);
    }

    public static class Emitter<T> {
        private final Observer<T> observer;
        private final Disposable disposable;
        private boolean done = false;

        Emitter(Observer<T> observer, Disposable disposable) {
            this.observer = observer;
            this.disposable = disposable;
        }

        public void onNext(T item) {
            if (!done && !disposable.isDisposed()) {
                observer.onNext(item);
            }
        }

        public void onError(Throwable t) {
            if (!done && !disposable.isDisposed()) {
                done = true;
                observer.onError(t);
            }
        }

        public void onComplete() {
            if (!done && !disposable.isDisposed()) {
                done = true;
                observer.onComplete();
            }
        }
    }

    private static class BooleanDisposable implements Disposable {
        private volatile boolean disposed = false;

        @Override
        public void dispose() {
            disposed = true;
        }

        @Override
        public boolean isDisposed() {
            return disposed;
        }
    }

    private static class DisposableObserver<T> implements Disposable, Observer<T> {
        private final Observer<T> downstream;
        private volatile boolean disposed = false;

        DisposableObserver(Observer<T> downstream) {
            this.downstream = downstream;
        }

        @Override
        public void onNext(T item) {
            if (!disposed) downstream.onNext(item);
        }

        @Override
        public void onError(Throwable t) {
            if (!disposed) {
                disposed = true;
                downstream.onError(t);
            }
        }

        @Override
        public void onComplete() {
            if (!disposed) {
                disposed = true;
                downstream.onComplete();
            }
        }

        @Override
        public void dispose() {
            disposed = true;
        }

        @Override
        public boolean isDisposed() {
            return disposed;
        }
    }

    private static class MapObservable<T, R> extends Observable<R> {
        private final Observable<T> source;
        private final Function<T, R> mapper;

        MapObservable(Observable<T> source, Function<T, R> mapper) {
            this.source = source;
            this.mapper = mapper;
        }

        @Override
        public Disposable subscribe(Observer<R> observer) {
            return source.subscribe(new MapObserver<>(observer, mapper));
        }

        private static class MapObserver<T, R> implements Observer<T> {
            private final Observer<R> downstream;
            private final Function<T, R> mapper;

            MapObserver(Observer<R> downstream, Function<T, R> mapper) {
                this.downstream = downstream;
                this.mapper = mapper;
            }

            @Override
            public void onNext(T item) {
                try {
                    downstream.onNext(mapper.apply(item));
                } catch (Throwable t) {
                    downstream.onError(t);
                }
            }

            @Override
            public void onError(Throwable t) {
                downstream.onError(t);
            }

            @Override
            public void onComplete() {
                downstream.onComplete();
            }
        }
    }

    private static class FilterObservable<T> extends Observable<T> {
        private final Observable<T> source;
        private final Predicate<T> predicate;

        FilterObservable(Observable<T> source, Predicate<T> predicate) {
            this.source = source;
            this.predicate = predicate;
        }

        @Override
        public Disposable subscribe(Observer<T> observer) {
            return source.subscribe(new FilterObserver<>(observer, predicate));
        }

        private static class FilterObserver<T> implements Observer<T> {
            private final Observer<T> downstream;
            private final Predicate<T> predicate;

            FilterObserver(Observer<T> downstream, Predicate<T> predicate) {
                this.downstream = downstream;
                this.predicate = predicate;
            }

            @Override
            public void onNext(T item) {
                try {
                    if (predicate.test(item)) {
                        downstream.onNext(item);
                    }
                } catch (Throwable t) {
                    downstream.onError(t);
                }
            }

            @Override
            public void onError(Throwable t) {
                downstream.onError(t);
            }

            @Override
            public void onComplete() {
                downstream.onComplete();
            }
        }
    }

    private static class SubscribeOnObservable<T> extends Observable<T> {
        private final Observable<T> source;
        private final Scheduler scheduler;

        SubscribeOnObservable(Observable<T> source, Scheduler scheduler) {
            this.source = source;
            this.scheduler = scheduler;
        }

        @Override
        public Disposable subscribe(Observer<T> observer) {
            BooleanDisposable d = new BooleanDisposable();
            scheduler.execute(() -> {
                if (!d.isDisposed()) {
                    source.subscribe(observer);
                }
            });
            return d;
        }
    }

    private static class ObserveOnObservable<T> extends Observable<T> {
        private final Observable<T> source;
        private final Scheduler scheduler;

        ObserveOnObservable(Observable<T> source, Scheduler scheduler) {
            this.source = source;
            this.scheduler = scheduler;
        }

        @Override
        public Disposable subscribe(Observer<T> observer) {
            return source.subscribe(new ObserveOnObserver<>(observer, scheduler));
        }

        private static class ObserveOnObserver<T> implements Observer<T> {
            private final Observer<T> downstream;
            private final Scheduler scheduler;

            ObserveOnObserver(Observer<T> downstream, Scheduler scheduler) {
                this.downstream = downstream;
                this.scheduler = scheduler;
            }

            @Override
            public void onNext(T item) {
                scheduler.execute(() -> downstream.onNext(item));
            }

            @Override
            public void onError(Throwable t) {
                scheduler.execute(() -> downstream.onError(t));
            }

            @Override
            public void onComplete() {
                scheduler.execute(downstream::onComplete);
            }
        }
    }

    private static class FlatMapObservable<T, R> extends Observable<R> {
        private final Observable<T> source;
        private final Function<T, Observable<R>> mapper;

        FlatMapObservable(Observable<T> source, Function<T, Observable<R>> mapper) {
            this.source = source;
            this.mapper = mapper;
        }

        @Override
        public Disposable subscribe(Observer<R> observer) {
            FlatMapObserver<T, R> flatObserver = new FlatMapObserver<>(observer, mapper);
            Disposable sourceDisposable = source.subscribe(flatObserver);
            return new CompositeDisposable(sourceDisposable, flatObserver);
        }

        private static class CompositeDisposable implements Disposable {
            private final Disposable[] disposables;
            private volatile boolean disposed = false;

            CompositeDisposable(Disposable... disposables) {
                this.disposables = disposables;
            }

            @Override
            public void dispose() {
                if (!disposed) {
                    disposed = true;
                    for (Disposable d : disposables) {
                        d.dispose();
                    }
                }
            }

            @Override
            public boolean isDisposed() {
                return disposed;
            }
        }

        private static class FlatMapObserver<T, R> implements Observer<T>, Disposable {
            private final Observer<R> downstream;
            private final Function<T, Observable<R>> mapper;
            private volatile int active = 0;
            private volatile boolean disposed = false;
            private volatile boolean sourceCompleted = false;

            FlatMapObserver(Observer<R> downstream, Function<T, Observable<R>> mapper) {
                this.downstream = downstream;
                this.mapper = mapper;
            }

            @Override
            public void onNext(T item) {
                if (disposed) return;

                try {
                    synchronized (this) {
                        active++;
                    }
                    Observable<R> inner = mapper.apply(item);

                    if (disposed) return;

                    inner.subscribe(new Observer<R>() {
                        @Override
                        public void onNext(R innerItem) {
                            if (!disposed) {
                                downstream.onNext(innerItem);
                            }
                        }

                        @Override
                        public void onError(Throwable t) {
                            if (!disposed) {
                                disposed = true;
                                downstream.onError(t);
                            }
                        }

                        @Override
                        public void onComplete() {
                            synchronized (FlatMapObserver.this) {
                                active--;
                                if (active == 0 && sourceCompleted && !disposed) {
                                    disposed = true;
                                    downstream.onComplete();
                                }
                            }
                        }
                    });
                } catch (Throwable t) {
                    if (!disposed) {
                        disposed = true;
                        downstream.onError(t);
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                if (!disposed) {
                    disposed = true;
                    downstream.onError(t);
                }
            }

            @Override
            public void onComplete() {
                sourceCompleted = true;
                synchronized (this) {
                    if (active == 0 && !disposed) {
                        disposed = true;
                        downstream.onComplete();
                    }
                }
            }

            @Override
            public void dispose() {
                disposed = true;
            }

            @Override
            public boolean isDisposed() {
                return disposed;
            }
        }
    }
}