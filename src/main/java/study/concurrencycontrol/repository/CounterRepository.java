package study.concurrencycontrol.repository;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import study.concurrencycontrol.entity.Counter;

import java.util.Optional;

public interface CounterRepository extends JpaRepository<Counter, Long> {

    Optional<Counter> findByName(String name);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT c FROM Counter c WHERE c.name = :name")
    Optional<Counter> findByNameWithPessimisticLock(@Param("name") String name);

    @Lock(LockModeType.OPTIMISTIC)
    @Query("SELECT c FROM Counter c WHERE c.name = :name")
    Optional<Counter> findByNameWithOptimisticLock(@Param("name") String name);

    @Modifying
    @Query("UPDATE Counter c SET c.count = c.count + 1 WHERE c.name = :name")
    int incrementByName(@Param("name") String name);
}
