package ru.customrx.core;

@FunctionalInterface
public interface Predicate<T> {
    boolean test(T t);
}
