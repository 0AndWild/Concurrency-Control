package study.concurrencycontrol.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import study.concurrencycontrol.config.TestConfig;
import study.concurrencycontrol.entity.Counter;
import study.concurrencycontrol.repository.CounterRepository;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = "spring.h2.console.enabled=true")
@Testcontainers
@Import(TestConfig.class)
class ConcurrencyTest {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);
    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }


    @Autowired
    private ConcurrencyService concurrencyService;

    @Autowired
    private ConcurrencyFacade concurrencyFacade;

    @Autowired
    private CounterRepository counterRepository;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    private static final String NO_SYNCHRONIZATION = "no_synchronization";
    private static final String SYNCHRONIZATION = "synchronization";
    private static final String REENTRANT = "Reentrant";
    private static final String JPA_PESSIMISTIC = "pessimistic";
    private static final String JPA_OPTIMISTIC = "optimistic";
    private static final String DATABASE_LOCK = "database-lock";
    private static final String REDIS_DISTRIBUTION = "redis-distribution";
    private static final String REDIS_ATOMIC = "redis-atomic";
    private static final String DATABASE_SERIALIZABLE = "database-serializable";

    private static final int THREAD_COUNT = 100;
    private static final int OPERATIONS_PER_THREAD = 10;
    private static final int EXPECTED_TOTAL = THREAD_COUNT * OPERATIONS_PER_THREAD;

    @BeforeEach
    void setUp() {
        // 데이터베이스 초기화
        counterRepository.deleteAll();

        // Redis 초기화
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    }

    @Test
    @DisplayName("1. 동기화 없음 - Race Condition 발생 확인")
    void testWithoutLock() throws InterruptedException {
        String counterName = NO_SYNCHRONIZATION;
        counterRepository.save(new Counter(counterName));

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger errors = new AtomicInteger(0);

        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        concurrencyService.incrementWithoutLock(counterName);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        Long finalCount = concurrencyService.getCountFromDatabase(counterName);
        System.out.println("동기화 없음 - 예상값: " + EXPECTED_TOTAL + ", 실제값: " + finalCount);

        // Race condition으로 인해 예상값보다 작을 것임
        assertTrue(finalCount < EXPECTED_TOTAL, "Race condition이 발생해야 함");
        System.out.println("에러 발생 횟수: " + errors.get());
    }

    @Test
    @DisplayName("2. synchronized 키워드 사용")
    void testWithSynchronized() throws InterruptedException {
        String counterName = SYNCHRONIZATION;
        counterRepository.save(new Counter(counterName));

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger errors = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        concurrencyFacade.incrementWithSynchronized(counterName);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        long endTime = System.currentTimeMillis();
        Long finalCount = concurrencyService.getCountFromDatabase(counterName);

        System.out.println("synchronized - 예상값: " + EXPECTED_TOTAL + ", 실제값: " + finalCount);
        System.out.println("실행 시간: " + (endTime - startTime) + "ms");
        System.out.println("에러 발생 횟수: " + errors.get());

        assertEquals(EXPECTED_TOTAL, finalCount, "synchronized는 정확한 결과를 보장해야 함");
    }

    @Test
    @DisplayName("3. ReentrantLock 사용")
    void testWithReentrantLock() throws InterruptedException {
        String counterName = REENTRANT;
        counterRepository.save(new Counter(counterName));

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger errors = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        concurrencyService.incrementWithReentrantLock(counterName);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        long endTime = System.currentTimeMillis();
        Long finalCount = concurrencyService.getCountFromDatabase(counterName);

        System.out.println("ReentrantLock - 예상값: " + EXPECTED_TOTAL + ", 실제값: " + finalCount);
        System.out.println("실행 시간: " + (endTime - startTime) + "ms");
        System.out.println("에러 발생 횟수: " + errors.get());

        assertEquals(EXPECTED_TOTAL, finalCount, "ReentrantLock은 정확한 결과를 보장해야 함");
    }

    @Test
    @DisplayName("4. JPA Pessimistic Lock 사용")
    void testWithPessimisticLock() throws InterruptedException {
        String counterName = JPA_PESSIMISTIC;
        counterRepository.save(new Counter(counterName));

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger errors = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        concurrencyService.incrementWithPessimisticLock(counterName);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        long endTime = System.currentTimeMillis();
        Long finalCount = concurrencyService.getCountFromDatabase(counterName);

        System.out.println("Pessimistic Lock - 예상값: " + EXPECTED_TOTAL + ", 실제값: " + finalCount);
        System.out.println("실행 시간: " + (endTime - startTime) + "ms");
        System.out.println("에러 발생 횟수: " + errors.get());

        assertEquals(EXPECTED_TOTAL, finalCount, "Pessimistic Lock은 정확한 결과를 보장해야 함");
    }

    @Test
    @DisplayName("5. JPA Optimistic Lock 사용")
    void testWithOptimisticLock() throws InterruptedException {
        String counterName = JPA_OPTIMISTIC;
        counterRepository.save(new Counter(counterName));

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger errors = new AtomicInteger(0);
        AtomicInteger retries = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        try {
                            concurrencyService.incrementWithOptimisticLock(counterName);
                        } catch (RuntimeException e) {
                            if (e.getMessage().contains("retry exceeded")) {
                                retries.incrementAndGet();
                            }
                            throw e;
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(60, TimeUnit.SECONDS);
        executor.shutdown();

        long endTime = System.currentTimeMillis();
        Long finalCount = concurrencyService.getCountFromDatabase(counterName);

        System.out.println("Optimistic Lock - 예상값: " + EXPECTED_TOTAL + ", 실제값: " + finalCount);
        System.out.println("실행 시간: " + (endTime - startTime) + "ms");
        System.out.println("에러 발생 횟수: " + errors.get());
        System.out.println("재시도 초과 횟수: " + retries.get());

        // Optimistic Lock은 높은 경합 상황에서 재시도 실패가 발생할 수 있음
        assertTrue(finalCount <= EXPECTED_TOTAL, "Optimistic Lock 결과는 예상값 이하여야 함");
    }

    @Test
    @DisplayName("6. Database Level Lock 사용")
    void testWithDatabaseLock() throws InterruptedException {
        String counterName = DATABASE_LOCK;
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger errors = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        concurrencyService.incrementWithDatabaseLock(counterName);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        long endTime = System.currentTimeMillis();
        Long finalCount = concurrencyService.getCountFromDatabase(counterName);

        System.out.println("Database Lock - 예상값: " + EXPECTED_TOTAL + ", 실제값: " + finalCount);
        System.out.println("실행 시간: " + (endTime - startTime) + "ms");
        System.out.println("에러 발생 횟수: " + errors.get());

        assertEquals(EXPECTED_TOTAL, finalCount, "Database Lock은 정확한 결과를 보장해야 함");
    }

    @Test
    @DisplayName("7. Redis Distributed Lock (Redisson) 사용")
    void testWithRedisLock() throws InterruptedException {
        String counterName = REDIS_DISTRIBUTION;
        counterRepository.save(new Counter(counterName));

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger errors = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        concurrencyService.incrementWithRedisLock(counterName);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(60, TimeUnit.SECONDS);
        executor.shutdown();

        long endTime = System.currentTimeMillis();
        Long finalCount = concurrencyService.getCountFromDatabase(counterName);

        System.out.println("Redis Lock - 예상값: " + EXPECTED_TOTAL + ", 실제값: " + finalCount);
        System.out.println("실행 시간: " + (endTime - startTime) + "ms");
        System.out.println("에러 발생 횟수: " + errors.get());

        assertEquals(EXPECTED_TOTAL, finalCount, "Redis Lock은 정확한 결과를 보장해야 함");
    }

    @Test
    @DisplayName("8. Redis 원자적 연산 사용")
    void testWithRedisAtomic() throws InterruptedException {
        String counterName = REDIS_ATOMIC;
        counterRepository.save(new Counter(counterName));

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger errors = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        concurrencyService.incrementWithRedisAtomic(counterName);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        long endTime = System.currentTimeMillis();
        Long finalCount = concurrencyService.getCountFromRedis(counterName);

        System.out.println("Redis Atomic - 예상값: " + EXPECTED_TOTAL + ", 실제값: " + finalCount);
        System.out.println("실행 시간: " + (endTime - startTime) + "ms");
        System.out.println("에러 발생 횟수: " + errors.get());

        assertEquals(EXPECTED_TOTAL, finalCount, "Redis Atomic 연산은 정확한 결과를 보장해야 함");
    }

    @Test
    @DisplayName("9. Database Serializable Isolation 사용")
    void testWithSerializableIsolation() throws InterruptedException {
        String counterName = DATABASE_SERIALIZABLE;
        counterRepository.save(new Counter(counterName));

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger errors = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        concurrencyService.incrementWithSerializableIsolation(counterName);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(60, TimeUnit.SECONDS);
        executor.shutdown();

        long endTime = System.currentTimeMillis();
        Long finalCount = concurrencyService.getCountFromDatabase(counterName);

        System.out.println("Serializable Isolation - 예상값: " + EXPECTED_TOTAL + ", 실제값: " + finalCount);
        System.out.println("실행 시간: " + (endTime - startTime) + "ms");
        System.out.println("에러 발생 횟수: " + errors.get());

        // Serializable 격리 수준에서는 높은 경합으로 인한 실패가 발생할 수 있음
        assertTrue(finalCount <= EXPECTED_TOTAL, "Serializable Isolation 결과는 예상값 이하여야 함");
        assertTrue(errors.get() > 0, "높은 경합 상황에서는 에러가 발생해야 함");
    }

    @Test
    @DisplayName("10. 성능 비교 테스트")
    void performanceComparison() throws InterruptedException {
        int reducedThreadCount = 50; // 성능 비교를 위해 쓰레드 수 줄임
        int reducedOperations = 5;   // 연산 수도 줄임

        System.out.println("\n=== 성능 비교 테스트 (쓰레드: " + reducedThreadCount + ", 연산: " + reducedOperations + ") ===");

        // 1. synchronized 성능 측정
        long syncTime = measurePerformance(SYNCHRONIZATION, reducedThreadCount, reducedOperations,
                () -> {
                    try {
                        concurrencyService.incrementWithSynchronized(SYNCHRONIZATION);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });

        // 2. ReentrantLock 성능 측정
        long lockTime = measurePerformance(REENTRANT, reducedThreadCount, reducedOperations,
                () -> {
                    try {
                        concurrencyService.incrementWithReentrantLock(REENTRANT);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });

        // 3. Redis Atomic 성능 측정
        long redisTime = measurePerformance(REDIS_ATOMIC, reducedThreadCount, reducedOperations,
                () -> {
                    try {
                        concurrencyService.incrementWithRedisAtomic(REDIS_ATOMIC);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });

        // 4. Database Lock 성능 측정
        long dbTime = measurePerformance(DATABASE_LOCK, reducedThreadCount, reducedOperations,
                () -> {
                    try {
                        concurrencyService.incrementWithDatabaseLock(DATABASE_LOCK);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });

        System.out.println("\n=== 성능 비교 결과 ===");
        System.out.println("synchronized:     " + syncTime + "ms");
        System.out.println("ReentrantLock:    " + lockTime + "ms");
        System.out.println("Redis Atomic:     " + redisTime + "ms");
        System.out.println("Database Lock:    " + dbTime + "ms");
    }

    private long measurePerformance(String counterName, int threadCount, int operations, Runnable task) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < operations; j++) {
                        task.run();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(60, TimeUnit.SECONDS);
        executor.shutdown();

        return System.currentTimeMillis() - startTime;
    }

    @Test
    @DisplayName("11. 스트레스 테스트 - 매우 높은 동시성")
    void stressTest() throws InterruptedException {
        int highThreadCount = 200;
        int highOperations = 5;
        String counterName = REDIS_ATOMIC;

        System.out.println("\n=== 스트레스 테스트 (쓰레드: " + highThreadCount + ") ===");

        ExecutorService executor = Executors.newFixedThreadPool(highThreadCount);
        CountDownLatch latch = new CountDownLatch(highThreadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < highThreadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < highOperations; j++) {
                        concurrencyService.incrementWithRedisAtomic(counterName);
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    failureCount.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(120, TimeUnit.SECONDS);
        executor.shutdown();

        long endTime = System.currentTimeMillis();
        Long finalCount = concurrencyService.getCountFromRedis(counterName);

        System.out.println("스트레스 테스트 결과:");
        System.out.println("- 예상 연산 수: " + (highThreadCount * highOperations));
        System.out.println("- 성공 연산 수: " + successCount.get());
        System.out.println("- 실패 연산 수: " + failureCount.get());
        System.out.println("- 최종 카운터 값: " + finalCount);
        System.out.println("- 실행 시간: " + (endTime - startTime) + "ms");
        System.out.println("- 초당 처리량: " + (successCount.get() * 1000.0 / (endTime - startTime)) + " ops/sec");

        assertEquals(successCount.get(), finalCount.longValue(), "성공한 연산 수와 최종 카운터 값이 일치해야 함");
    }

    @Test
    @DisplayName("12. 모든 동시성 제어 방법 한번에 비교")
    void compareAllMethods() throws InterruptedException {
        int testThreadCount = 20;
        int testOperations = 10;

        System.out.println("\n=== 전체 동시성 제어 방법 비교 ===");
        System.out.println("테스트 조건: " + testThreadCount + " 쓰레드 x " + testOperations + " 연산");

        // 각 방법별 테스트 결과 저장
        Map<String, TestResult> results = new HashMap<>();

        // 1. synchronized
        results.put(SYNCHRONIZATION, runComparisonTest(SYNCHRONIZATION, testThreadCount, testOperations,
                () -> {
                    try {
                        concurrencyService.incrementWithSynchronized(SYNCHRONIZATION);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                },
                () -> concurrencyService.getCountFromDatabase(SYNCHRONIZATION)));

        // 2. ReentrantLock
        results.put(REENTRANT, runComparisonTest(REENTRANT, testThreadCount, testOperations,
                () -> {
                    try {
                        concurrencyService.incrementWithReentrantLock(REENTRANT);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                },
                () -> concurrencyService.getCountFromDatabase(REENTRANT)));

        // 3. Pessimistic Lock
        results.put(JPA_PESSIMISTIC, runComparisonTest(JPA_PESSIMISTIC, testThreadCount, testOperations,
                () -> {
                    try {
                        concurrencyService.incrementWithPessimisticLock(JPA_PESSIMISTIC);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                },
                () -> concurrencyService.getCountFromDatabase(JPA_PESSIMISTIC)));

        // 4. Database Lock
        results.put(DATABASE_LOCK, runComparisonTest(DATABASE_LOCK, testThreadCount, testOperations,
                () -> {
                    try {
                        concurrencyService.incrementWithDatabaseLock(DATABASE_LOCK);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                },
                () -> concurrencyService.getCountFromDatabase(DATABASE_LOCK)));

        // 5. Redis Lock
        results.put(REDIS_DISTRIBUTION, runComparisonTest(REDIS_DISTRIBUTION, testThreadCount, testOperations,
                () -> {
                    try {
                        concurrencyService.incrementWithRedisLock(REDIS_DISTRIBUTION);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                },
                () -> concurrencyService.getCountFromDatabase(REDIS_DISTRIBUTION)));

        // 6. Redis Atomic
        results.put(REDIS_ATOMIC, runComparisonTest(REDIS_ATOMIC, testThreadCount, testOperations,
                () -> {
                    try {
                        concurrencyService.incrementWithRedisAtomic(REDIS_ATOMIC);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                },
                () -> concurrencyService.getCountFromRedis(REDIS_ATOMIC)));

        // 결과 출력
        System.out.println("\n=== 비교 결과 ===");
        System.out.printf("%-15s %-10s %-10s %-10s %-15s\n", "방법", "시간(ms)", "정확성", "에러수", "처리량(ops/s)");
        System.out.println("─".repeat(70));

        results.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.comparingLong(r -> r.executionTime)))
                .forEach(entry -> {
                    TestResult result = entry.getValue();
                    String accuracy = result.isAccurate ? "✓" : "✗";
                    double throughput = (double) result.expectedOperations * 1000 / result.executionTime;
                    System.out.printf("%-15s %-10d %-10s %-10d %-15.1f\n",
                            entry.getKey(), result.executionTime, accuracy, result.errors, throughput);
                });
    }

    private TestResult runComparisonTest(String counterName, int threadCount, int operations,
                                         Runnable task, Supplier<Long> resultGetter) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger errors = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < operations; j++) {
                        task.run();
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(60, TimeUnit.SECONDS);
        executor.shutdown();

        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        long expectedCount = (long) threadCount * operations;
        long actualCount = resultGetter.get();
        boolean isAccurate = actualCount == expectedCount;

        return new TestResult(executionTime, expectedCount, actualCount, isAccurate, errors.get());
    }

    private static class TestResult {
        final long executionTime;
        final long expectedOperations;
        final long actualResult;
        final boolean isAccurate;
        final int errors;

        TestResult(long executionTime, long expectedOperations, long actualResult, boolean isAccurate, int errors) {
            this.executionTime = executionTime;
            this.expectedOperations = expectedOperations;
            this.actualResult = actualResult;
            this.isAccurate = isAccurate;
            this.errors = errors;
        }
    }
}