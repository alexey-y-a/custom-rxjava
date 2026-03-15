package ru.customrx;

import ru.customrx.core.Observable;
import ru.customrx.core.Observer;
import ru.customrx.schedulers.Schedulers;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Custom RxJava Demo\n");

        System.out.println("1. Basic operators (synchronous):");
        Observable.just(1, 2, 3, 4, 5)
                .filter(i -> i % 2 == 0)
                .map(i -> "Even number: " + i)
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
                        System.out.println("  " + item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        System.err.println("Error: " + t.getMessage());
                    }

                    @Override
                    public void onComplete() {
                        System.out.println("  Completed");
                    }
                });

        System.out.println("\n2. subscribeOn + observeOn (async):");
        Observable.just("heavy operation")
                .subscribeOn(Schedulers.io())
                .map(s -> {
                    System.out.println("  Processing on: " + Thread.currentThread().getName());
                    return s.toUpperCase();
                })
                .observeOn(Schedulers.computation())
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
                        System.out.println("  Received on: " + Thread.currentThread().getName() + " -> " + item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        System.err.println("Error: " + t.getMessage());
                    }

                    @Override
                    public void onComplete() {
                        System.out.println("  Async completed");
                    }
                });

        System.out.println("\n3. flatMap example:");
        Observable.just(1, 2, 3)
                .flatMap(i -> Observable.just(i, i * 10))
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                        System.out.println("  flatMap result: " + item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        System.err.println("Error: " + t.getMessage());
                    }

                    @Override
                    public void onComplete() {
                        System.out.println("  flatMap completed");
                    }
                });

        System.out.println("\n4. Error handling:");
        Observable.just(1, 2, 0, 3)
                .map(i -> 10 / i)
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                        System.out.println("  Result: " + item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        System.out.println("  Error caught: " + t.getMessage());
                    }

                    @Override
                    public void onComplete() {
                        System.out.println("  Error handling completed");
                    }
                });

        Thread.sleep(1000);
        System.out.println("\nDemo finished");
    }
}