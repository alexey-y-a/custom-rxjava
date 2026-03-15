package ru.customrx.core;

@FunctionalInterface
public interface Function<T, R> {
    R apply(T t);
}
