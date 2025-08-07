package study.concurrencycontrol.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
public class ConcurrencyFacade {

    private final ConcurrencyService concurrencyService;

    public synchronized void incrementWithSynchronized(String counterName) throws InterruptedException {
        concurrencyService.incrementWithSynchronized(counterName);
    }
}
