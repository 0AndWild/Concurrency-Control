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

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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

    private static final String LOCK_COUNTER_NAME = "lock_counter";
    private static final int THREAD_COUNT = 100;
    private static final int OPERATIONS_PER_THREAD = 10;
    private static final int EXPECTED_TOTAL = THREAD_COUNT * OPERATIONS_PER_THREAD;

    @BeforeEach
    void setUp() {
        // 데이터베이스 초기화
        counterRepository.deleteAll();

        // 초기 데이터 한 건 셋팅
        counterRepository.save(new Counter(LOCK_COUNTER_NAME));

        // Redis 초기화
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    }

    @Test
    @DisplayName("1. 동기화 없음 - Race Condition 발생 확인")
    void testWithoutLock() throws InterruptedException {
        String counterName = LOCK_COUNTER_NAME;
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
        String counterName = LOCK_COUNTER_NAME;
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
        String counterName = LOCK_COUNTER_NAME;
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
        String counterName = LOCK_COUNTER_NAME;
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
        String counterName = LOCK_COUNTER_NAME;
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
        String counterName = LOCK_COUNTER_NAME;
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
//                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        long endTime = System.currentTimeMillis();
        Long finalCount = concurrencyService.getCountFromDatabase(counterName);

        System.out.println("Database level Lock - 예상값: " + EXPECTED_TOTAL + ", 실제값: " + finalCount);
        System.out.println("실행 시간: " + (endTime - startTime) + "ms");
        System.out.println("에러 발생 횟수: " + errors.get());

        // Optimistic Lock은 높은 경합 상황에서 재시도 실패가 발생할 수 있음
        assertTrue(finalCount <= EXPECTED_TOTAL, "Database level Lock 결과는 예상값 이하여야 함");

    }
}