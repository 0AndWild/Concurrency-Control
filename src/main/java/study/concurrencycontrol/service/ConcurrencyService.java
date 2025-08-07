package study.concurrencycontrol.service;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import study.concurrencycontrol.entity.Counter;
import study.concurrencycontrol.repository.CounterRepository;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class ConcurrencyService {

    @Autowired
    private CounterRepository counterRepository;

    @Autowired
    private RedissonClient redissonClient;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    // Java ReentrantLock 사용
    private final ReentrantLock javaLock = new ReentrantLock();

    /**
     * 1. 동기화 없음 (Race Condition 발생)
     */
    @Transactional
    public void incrementWithoutLock(String counterName) throws InterruptedException {
        Counter counter = counterRepository.findByName(counterName)
                .orElse(new Counter(counterName));

        // 실제 업무 처리 시뮬레이션
        Thread.sleep(10);

        counter.increment();
        counterRepository.save(counter);
    }

    /**
     * 2. Java synchronized 사용
     */
    @Transactional
    public void incrementWithSynchronized(String counterName) throws InterruptedException {
        Counter counter = counterRepository.findByName(counterName)
                .orElse(new Counter(counterName));

        Thread.sleep(10);

        counter.increment();
        counterRepository.save(counter);
    }

    /**
     * 3. Java ReentrantLock 사용
     */
    @Transactional
    public void incrementWithReentrantLock(String counterName) throws InterruptedException {
        javaLock.lock();
        try {
            Counter counter = counterRepository.findByName(counterName)
                    .orElse(new Counter(counterName));

            Thread.sleep(10);

            counter.increment();
            counterRepository.save(counter);
        } finally {
            javaLock.unlock();
        }
    }

    /**
     * 4. JPA Pessimistic Lock 사용
     */
    @Transactional
    public void incrementWithPessimisticLock(String counterName) throws InterruptedException {
        Counter counter = counterRepository.findByNameWithPessimisticLock(counterName)
                .orElse(new Counter(counterName));

        Thread.sleep(10);

        counter.increment();
        counterRepository.save(counter);
    }

    /**
     * 5. JPA Optimistic Lock 사용 (재시도 로직 포함)
     */
    @Transactional
    public void incrementWithOptimisticLock(String counterName) throws InterruptedException {
        int maxRetries = 3;
        int attempt = 0;

        while (attempt < maxRetries) {
            try {
                Counter counter = counterRepository.findByNameWithOptimisticLock(counterName)
                        .orElse(new Counter(counterName));

                Thread.sleep(10);

                counter.increment();
                counterRepository.save(counter);
                return; // 성공시 종료

            } catch (ObjectOptimisticLockingFailureException e) {
                attempt++;
                if (attempt >= maxRetries) {
                    throw new RuntimeException("Optimistic lock retry exceeded", e);
                }
                // 재시도 전 잠시 대기
                Thread.sleep(50);
            }
        }
    }

    /**
     * 6. Database Level Lock 사용 (@Modifying 쿼리)
     */
    @Transactional
    public void incrementWithDatabaseLock(String counterName) throws InterruptedException {
        // 카운터가 없으면 먼저 생성
        if (counterRepository.findByName(counterName).isEmpty()) {
            counterRepository.save(new Counter(counterName));
        }

        Thread.sleep(10);

        counterRepository.incrementByName(counterName);
    }

    /**
     * 7. Redis Distributed Lock (Redisson) 사용
     */
    @Transactional
    public void incrementWithRedisLock(String counterName) throws InterruptedException {
        String lockKey = "lock:" + counterName;
        RLock lock = redissonClient.getLock(lockKey);

        try {
            // 10초 동안 락 획득 시도, 최대 5초 동안 락 유지
            if (lock.tryLock(10, 5, TimeUnit.SECONDS)) {
                Counter counter = counterRepository.findByName(counterName)
                        .orElse(new Counter(counterName));

                Thread.sleep(10);

                counter.increment();
                counterRepository.save(counter);
            } else {
                throw new RuntimeException("Failed to acquire Redis lock");
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /**
     * 8. Redis를 사용한 단순 원자적 연산
     */
    public void incrementWithRedisAtomic(String counterName) throws InterruptedException {
        String key = "atomic:" + counterName;

        Thread.sleep(10);

        redisTemplate.opsForValue().increment(key);
    }

    /**
     * 9. Database Transaction Isolation Level 사용
     */
    @Transactional(isolation = Isolation.SERIALIZABLE)
    public void incrementWithSerializableIsolation(String counterName) throws InterruptedException {
        Counter counter = counterRepository.findByName(counterName)
                .orElse(new Counter(counterName));

        Thread.sleep(10);

        counter.increment();
        counterRepository.save(counter);
    }

    // 카운터 값 조회 메소드들
    public Long getCountFromDatabase(String counterName) {
        return counterRepository.findByName(counterName)
                .map(Counter::getCount)
                .orElse(0L);
    }

    public Long getCountFromRedis(String counterName) {
        String key = "atomic:" + counterName;
        String value = redisTemplate.opsForValue().get(key);
        return value != null ? Long.valueOf(value) : 0L;
    }
}
