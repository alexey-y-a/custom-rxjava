# Custom RxJava

Проект по реализации собственной версии библиотеки RxJava.

Цель работы — создать систему реактивных потоков с возможностью управления потоками выполнения (Schedulers) и 
обработки событий с использованием паттерна «Наблюдатель» (Observer pattern).

## Возможности
- **Core**: Observable, Observer, Disposable, Emitter
- **Операторы**: map, filter, flatMap
- **Schedulers**: IO, Computation, Single на базе ExecutorService
- **Threading**: subscribeOn и observeOn для управления потоками
- **Отмена подписок**: через Disposable

---

## Содержание
- [Стек технологий](#стек-технологий)
- [Архитектура системы](#архитектура-системы)
- [Принципы работы Schedulers](#принципы-работы-schedulers)
- [Реализованные операторы](#реализованные-операторы)
- [Обработка ошибок и Disposable](#обработка-ошибок-и-disposable)
- [Процесс тестирования](#процесс-тестирования)
- [Исследование производительности](#исследование-производительности)
- [Примеры использования](#примеры-использования)
- [Запуск проекта](#запуск-проекта)

---

## Стек технологий
| Технология | Версия | Назначение |
|------------|--------|------------|
| **Java** | 21 | Основной язык |
| **JUnit 5** | 5.10.2 | Модульное тестирование |
| **Maven** | 3.9+ | Сборка проекта |

---

## Архитектура системы

### Базовые компоненты
| Компонент | Назначение | Реализация |
|-----------|------------|------------|
| **Observable** | Источник данных, паттерн Наблюдатель | `Observable.java` |
| **Observer** | Потребитель данных | `Observer.java` |
| **Disposable** | Отмена подписки | `Disposable.java` |
| **Emitter** | Эмиссия данных с проверкой отмены | `Observable.Emitter` |

### Принцип работы
1. **Создание**: `Observable.create(emitter -> {...})` или `Observable.just(...)`
2. **Трансформация**: применение операторов `map`/`filter`/`flatMap`
3. **Подписка**: вызов `subscribe(Observer)`
4. **Иммутабельность**: каждый оператор создает новый Observable
5. **Отмена**: вызов `dispose()` прекращает эмиссию

---

## Принципы работы Schedulers

### Реализованные планировщики

| Scheduler | Пул потоков | Применение |
|-----------|-------------|------------|
| **IO** | `CachedThreadPool` | Блокирующие операции (сеть, файлы, БД) |
| **Computation** | `FixedThreadPool` (кол-во ядер CPU) | CPU-интенсивные вычисления |
| **Single** | `SingleThreadExecutor` | Последовательная обработка, гарантия порядка |

### Методы управления потоками

```
Observable.just("data")
    .subscribeOn(Schedulers.io())         // эмиссия в IO потоке
    .map(data -> process(data))           // обработка в том же потоке
    .observeOn(Schedulers.computation())  // переключение для результата
```

**Различия:**
```
subscribeOn влияет на поток всей цепочки до первого observeOn

observeOn переключает поток для последующих операторов

Несколько observeOn могут переключать поток многократно
```
---

## Реализованные операторы

### map
**Преобразует каждый элемент потока.**
```
Observable.just(1, 2, 3)
    .map(i -> "Number: " + i)
    .subscribe(new Observer<String>() {
        @Override
        public void onNext(String item) {
            System.out.println(item);
        }
        @Override
        public void onError(Throwable t) {}
        @Override
        public void onComplete() {}
    });
// Output: Number: 1, Number: 2, Number: 3
```

### filter
**Пропускает только элементы, удовлетворяющие условию.**
```
Observable.just(1, 2, 3, 4)
    .filter(i -> i % 2 == 0)
    .subscribe(new Observer<Integer>() {
        @Override
        public void onNext(Integer item) {
            System.out.println(item);
        }
        @Override
        public void onError(Throwable t) {}
        @Override
        public void onComplete() {}
    });
// Output: 2, 4
```

### flatMap
**Преобразует элемент в новый Observable и "разворачивает" его.**
```
Observable.just(1, 2)
    .flatMap(i -> Observable.just(i, i * 10))
    .subscribe(new Observer<Integer>() {
        @Override
        public void onNext(Integer item) {
            System.out.println(item);
        }
        @Override
        public void onError(Throwable t) {}
        @Override
        public void onComplete() {}
    });
// Output: 1, 10, 2, 20 (порядок может меняться)
```
---

## Обработка ошибок и Disposable

### Обработка ошибок

Ошибки могут возникать:

* В эмиттере при создании Observable
* В операторах map/filter/flatMap
* Во внутренних Observable при flatMap

Все ошибки передаются в onError() и завершают цепочку.
```
Observable.just(1, 2, 0, 3)
    .map(i -> 10 / i)
    .subscribe(new Observer<Integer>() {
        @Override
        public void onNext(Integer item) {
            System.out.println("Result: " + item);
        }
        @Override
        public void onError(Throwable t) {
            System.out.println("Error: " + t.getMessage());
        }
        @Override
        public void onComplete() {}
    });
```

* Disposable (отмена подписки)
```
Disposable disposable = observable.subscribe(new Observer<T>() {
    @Override
    public void onNext(T item) { ... }
    @Override
    public void onError(Throwable t) { ... }
    @Override
    public void onComplete() { ... }
});
// позже
disposable.dispose();
```
---

## Процесс тестирования

### Покрытые сценарии

| Тест | Что проверяет | Количество тестов |
|------|---------------|---------------|
| **BasicObservableTest** | `create()`, `just()`, подписка | 2 |
| **DisposableTest** | отмена подписки, отсутствие событий после `dispose()` | 1 |
| **FlatMapTest** | корректная работа `flatMap` (все 4 элемента) | 1 |
| **SchedulerTest** | `subscribeOn`, `observeOn`, переключение потоков | 2 |
| **ErrorHandlingTest** | обработка ошибок в `create()` и `map()` | 2 |
| **ИТОГО** | | 8 |

---

## Исследование производительности

### Влияние типа Scheduler на скорость

| Scheduler | Время на 1000 задач | Комментарий |
|------|---------------------|-------------|
| IO | ~15 мс | Быстрый старт, создает новые потоки по мере необходимости |
| Computation | ~20 мс | Фиксированный пул размером с количество ядер CPU |
| Single | ~25 мс | Последовательное выполнение в одном потоке |


Оптимальные значения
* IO Scheduler: для задач с блокировками (сеть, файлы)
* Computation Scheduler: для CPU-интенсивных задач
* Single Scheduler: когда важен порядок обработки
---

## Примеры использования

### Пример 1: Базовые операторы
```
Observable.just(1, 2, 3, 4, 5)
    .filter(i -> i % 2 == 0)
    .map(i -> "Even: " + i)
    .subscribe(new Observer<String>() {
        @Override
        public void onNext(String item) {
            System.out.println(item);
        }
        @Override
        public void onError(Throwable t) {}
        @Override
        public void onComplete() {}
    });
```

### Пример 2: Асинхронная обработка
```
Observable.just("heavy task")
    .subscribeOn(Schedulers.io())
    .map(task -> {
        System.out.println("Processing on: " + Thread.currentThread().getName());
        return task.toUpperCase();
    })
    .observeOn(Schedulers.computation())
    .subscribe(new Observer<String>() {
        @Override
        public void onNext(String result) {
            System.out.println("Result on: " + Thread.currentThread().getName());
        }
        @Override
        public void onError(Throwable t) {}
        @Override
        public void onComplete() {}
    });
```

### Пример 3: flatMap с несколькими значениями
```
Observable.just(1, 2, 3)
    .flatMap(i -> Observable.just(i, i * 10))
    .subscribe(new Observer<Integer>() {
        @Override
        public void onNext(Integer item) {
            System.out.print(item + " ");
        }
        @Override
        public void onError(Throwable t) {}
        @Override
        public void onComplete() {
            System.out.println();
        }
    });
```

### Пример 4: Обработка ошибок
```
Observable.just(1, 2, 0, 3)
    .map(i -> 10 / i)
    .subscribe(new Observer<Integer>() {
        @Override
        public void onNext(Integer item) {
            System.out.println("Result: " + item);
        }
        @Override
        public void onError(Throwable t) {
            System.err.println("Error: " + t.getMessage());
        }
        @Override
        public void onComplete() {}
    });
```

### Пример 5: Отмена подписки
```
Disposable disposable = Observable.create(emitter -> {
    for (int i = 1; i <= 10; i++) {
        if (Thread.currentThread().isInterrupted()) return;
        emitter.onNext(i);
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
    }
    emitter.onComplete();
}).subscribe(new Observer<Integer>() {
    @Override
    public void onNext(Integer item) {
        System.out.println("Received: " + item);
    }
    @Override
    public void onError(Throwable t) {}
    @Override
    public void onComplete() {}
});

Thread.sleep(350);
disposable.dispose();
System.out.println("Disposed");
```
---

## Запуск проекта

1. Быстрая проверка компиляции: `mvn clean compile`
2. Запуск тестов: `mvn clean test`
3. Запуск демонстрационной программы: `mvn exec:java -Dexec.mainClass="ru.customrx.Main"`

или

1. Полная сборка и созданием JAR: `mvn clean package`
2. Запуск демо из собранного JAR: `java -jar target/custom-rxjava-1.0-SNAPSHOT.jar`


