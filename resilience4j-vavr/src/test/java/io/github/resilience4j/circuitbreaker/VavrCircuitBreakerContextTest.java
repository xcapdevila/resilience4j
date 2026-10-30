/*
 *
 *  Copyright 2026 authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */
package io.github.resilience4j.circuitbreaker;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.CLOSED;
import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.HALF_OPEN;
import static org.assertj.core.api.Assertions.assertThat;

import io.vavr.CheckedConsumer;
import io.vavr.control.Either;
import io.vavr.control.Try;
import java.util.List;

class VavrCircuitBreakerContextTest {
    private static final Duration DWELL = Duration.ofSeconds(1);

    @Test
    void everyVavrDecoratorKeepsItsAdmissionOwnerWhenAnotherCallAdvancesRecovery() throws Throwable {
        List<CheckedConsumer<Fixture>> invocations = List.of(
            fixture -> VavrCircuitBreaker.executeCheckedSupplier(fixture.breaker, () -> {
                fixture.advanceToHundredPercent();
                return "late";
            }),
            fixture -> VavrCircuitBreaker.executeCheckedRunnable(fixture.breaker,
                fixture::advanceToHundredPercent),
            fixture -> VavrCircuitBreaker.decorateCheckedConsumer(fixture.breaker,
                ignored -> fixture.advanceToHundredPercent()).accept("input"),
            fixture -> VavrCircuitBreaker.decorateCheckedFunction(fixture.breaker, ignored -> {
                fixture.advanceToHundredPercent();
                return "late";
            }).apply("input"),
            fixture -> VavrCircuitBreaker.executeEitherSupplier(fixture.breaker, () -> {
                fixture.advanceToHundredPercent();
                return Either.right("late");
            }),
            fixture -> VavrCircuitBreaker.executeTrySupplier(fixture.breaker, () -> {
                fixture.advanceToHundredPercent();
                return Try.success("late");
            }));

        for (CheckedConsumer<Fixture> invocation : invocations) {
            Fixture fixture = new Fixture();
            invocation.accept(fixture);
            fixture.assertCurrentWindowEmpty();
            fixture.validateCurrentPhase();
        }
    }

    private static final class Fixture {
        private final MutableClock clock = new MutableClock();
        private final CircuitBreaker breaker = CircuitBreaker.of("contextual-integration",
            CircuitBreakerConfig.custom().clock(clock).initialState(HALF_OPEN)
                .halfOpenRecoveryConfig(HalfOpenRecoveryConfig.custom()
                    .initialTrafficPercentage(75).trafficPercentageStep(25)
                    .minimumPhaseDuration(DWELL).minimumNumberOfCalls(1).build()).build());

        private void advanceToHundredPercent() {
            CallPermission permission = null;
            for (int attempt = 0; attempt < 100 && permission == null; attempt++) {
                permission = breaker.tryAcquireCallPermission();
            }
            assertThat(permission).isNotNull();
            clock.advance(DWELL);
            permission.onSuccess(0, TimeUnit.NANOSECONDS);
            assertCurrentWindowEmpty();
        }

        private void assertCurrentWindowEmpty() {
            assertThat(breaker.getState()).isEqualTo(HALF_OPEN);
            assertThat(breaker.getHalfOpenPhase().getPermittedTrafficPercentage()).isEqualTo(100);
            assertThat(breaker.getMetrics().getNumberOfBufferedCalls()).isZero();
        }

        private void validateCurrentPhase() {
            breaker.executeSupplier(() -> {
                clock.advance(DWELL);
                return "current";
            });
            assertThat(breaker.getState()).isEqualTo(CLOSED);
        }
    }

    private static final class MutableClock extends Clock {
        private final AtomicReference<Instant> now;
        private final ZoneId zone;

        private MutableClock() {
            this(new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z")), ZoneOffset.UTC);
        }

        private MutableClock(AtomicReference<Instant> now, ZoneId zone) {
            this.now = now;
            this.zone = zone;
        }

        private void advance(Duration duration) {
            now.updateAndGet(value -> value.plus(duration));
        }

        @Override
        public ZoneId getZone() {
            return zone;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return new MutableClock(now, zone);
        }

        @Override
        public Instant instant() {
            return now.get();
        }
    }
}
