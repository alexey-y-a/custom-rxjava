package ru.customrx.core;

public interface Disposable {
    void dispose();
    boolean isDisposed();
}
