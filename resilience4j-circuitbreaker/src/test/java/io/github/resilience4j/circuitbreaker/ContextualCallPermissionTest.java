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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.HALF_OPEN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ContextualCallPermissionTest {

    @Test
    void strictRecoveryRejectsEveryTokenlessAdmissionAndCompletionMethod() {
        CircuitBreaker breaker = breaker(new MutableClock(), config -> { });
        assertThatThrownBy(breaker::acquirePermission).isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("CallPermission");
        assertThatThrownBy(breaker::tryAcquirePermission).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(breaker::releasePermission).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> breaker.onSuccess(0, TimeUnit.NANOSECONDS))
            .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> breaker.onError(0, TimeUnit.NANOSECONDS, new RuntimeException()))
            .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> breaker.onResult(0, TimeUnit.NANOSECONDS, "result"))
            .isInstanceOf(IllegalStateException.class);
        assertThat(breaker.getAdmissionMetrics().getNumberOfAttempts()).isZero();
    }

    @Test
    void standardNativeBreakerReusesItsForwardingHandleAndKeepsLegacyMethodsAvailable() {
        CircuitBreaker breaker = CircuitBreaker.ofDefaults("standard");
        assertThat(breaker.acquireCallPermission()).isSameAs(breaker.tryAcquireCallPermission());
        breaker.acquirePermission();
        breaker.onSuccess(0, TimeUnit.NANOSECONDS);
        assertThat(breaker.getMetrics().getNumberOfSuccessfulCalls()).isEqualTo(1);
    }

    @Test
    void customImplementationsInheritForwardingDefaults() {
        CircuitBreaker breaker = mock(CircuitBreaker.class, CALLS_REAL_METHODS);
        when(breaker.tryAcquirePermission()).thenReturn(true, false);
        CallPermission permission = breaker.tryAcquireCallPermission();
        assertThat(permission).isNotNull();
        permission.onResult(7, TimeUnit.MILLISECONDS, "result");
        verify(breaker).onResult(7, TimeUnit.MILLISECONDS, "result");
        assertThat(breaker.tryAcquireCallPermission()).isNull();
        breaker.acquireCallPermission().release();
        verify(breaker).acquirePermission();
        verify(breaker).releasePermission();
    }

    @Test
    void callsAdmittedClosedCannotSupplyRecoverySamplesOrReopenTheirReplacement() {
        MutableClock clock = new MutableClock();
        CircuitBreaker breaker = breaker(clock, config -> config.minimumNumberOfCalls(1));
        AtomicInteger errors = new AtomicInteger();
        breaker.getEventPublisher().onError(event -> errors.incrementAndGet());
        CallPermission closed = breaker.acquireCallPermission();
        breaker.reset();
        enterRecovery(breaker);
        closed.onError(0, TimeUnit.NANOSECONDS, new IllegalStateException("old closed failure"));
        assertThat(breaker.getState()).isEqualTo(HALF_OPEN);
        assertThat(breaker.getMetrics().getNumberOfBufferedCalls()).isZero();
        assertThat(errors.get()).isEqualTo(1);
    }

    @Test
    void previousVisitToTheSamePercentageCannotValidateAStepBackVisit() {
        MutableClock clock = new MutableClock();
        CircuitBreaker breaker = breaker(clock, config -> config.halfOpenRecoveryConfig(
            recovery().failureStrategy(HalfOpenRecoveryConfig.FailureStrategy.STEP_BACK).build()));
        enterRecovery(breaker);
        CallPermission oldA = admit(breaker);
        CallPermission oldB = admit(breaker);
        completeHealthyPhase(breaker, clock);
        assertThat(breaker.getHalfOpenPhase().getPermittedTrafficPercentage()).isEqualTo(100);
        admit(breaker).onError(0, TimeUnit.NANOSECONDS, new IllegalStateException());
        admit(breaker).onError(0, TimeUnit.NANOSECONDS, new IllegalStateException());
        assertThat(breaker.getHalfOpenPhase().getPermittedTrafficPercentage()).isEqualTo(50);
        clock.advance();
        oldA.onSuccess(0, TimeUnit.NANOSECONDS);
        oldB.onSuccess(0, TimeUnit.NANOSECONDS);
        assertThat(breaker.getMetrics().getNumberOfBufferedCalls()).isZero();
        assertThat(breaker.getHalfOpenPhase().getPermittedTrafficPercentage()).isEqualTo(50);
        admit(breaker).onSuccess(0, TimeUnit.NANOSECONDS);
        admit(breaker).onSuccess(0, TimeUnit.NANOSECONDS);
        assertThat(breaker.getHalfOpenPhase().getPermittedTrafficPercentage()).isEqualTo(100);
    }

    @Test
    void anOutcomeOrReleaseConsumesTheHandleOnce() {
        CircuitBreaker breaker = breaker(new MutableClock(), config -> { });
        enterRecovery(breaker);
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();
        breaker.getEventPublisher().onSuccess(event -> successes.incrementAndGet());
        breaker.getEventPublisher().onError(event -> errors.incrementAndGet());
        CallPermission completed = admit(breaker);
        completed.onResult(0, TimeUnit.NANOSECONDS, "success");
        completed.onError(0, TimeUnit.NANOSECONDS, new RuntimeException());
        completed.onSuccess(0, TimeUnit.NANOSECONDS);
        completed.release();
        CallPermission released = admit(breaker);
        released.release();
        released.onSuccess(0, TimeUnit.NANOSECONDS);
        assertThat(successes.get()).isEqualTo(1);
        assertThat(errors.get()).isZero();
        assertThat(breaker.getMetrics().getNumberOfBufferedCalls()).isEqualTo(1);
    }

    @Test
    void ignoredOutcomesConsumeTheHandleWithoutSupplyingASample() {
        CircuitBreaker breaker = breaker(new MutableClock(), config ->
            config.ignoreExceptions(IllegalArgumentException.class));
        enterRecovery(breaker);
        AtomicInteger ignored = new AtomicInteger();
        breaker.getEventPublisher().onIgnoredError(event -> ignored.incrementAndGet());
        CallPermission permission = admit(breaker);
        permission.onError(0, TimeUnit.NANOSECONDS,
            new CompletionException(new IllegalArgumentException("ignored")));
        permission.onSuccess(0, TimeUnit.NANOSECONDS);
        assertThat(ignored.get()).isEqualTo(1);
        assertThat(breaker.getMetrics().getNumberOfBufferedCalls()).isZero();
    }

    @Test
    void staleFailuresAndRecordedResultsRemainEventsWithoutPopulatingTheNextPhase() {
        MutableClock clock = new MutableClock();
        CircuitBreaker breaker = breaker(clock, config -> config.recordResult("failed"::equals));
        enterRecovery(breaker);
        AtomicInteger errors = new AtomicInteger();
        breaker.getEventPublisher().onError(event -> errors.incrementAndGet());
        CallPermission oldError = admit(breaker);
        CallPermission oldResult = admit(breaker);
        completeHealthyPhase(breaker, clock);
        oldError.onError(0, TimeUnit.NANOSECONDS, new IllegalStateException("old failure"));
        oldResult.onResult(0, TimeUnit.NANOSECONDS, "failed");
        assertThat(errors.get()).isEqualTo(2);
        assertThat(breaker.getState()).isEqualTo(HALF_OPEN);
        assertThat(breaker.getHalfOpenPhase().getPermittedTrafficPercentage()).isEqualTo(100);
        assertThat(breaker.getMetrics().getNumberOfBufferedCalls()).isZero();
    }

    @Test
    void futureCompletionRetainsItsOwnerAndCancellationSuppliesNoHealthSample() throws Exception {
        MutableClock clock = new MutableClock();
        CircuitBreaker breaker = breaker(clock, config -> { });
        enterRecovery(breaker);
        AtomicInteger successes = new AtomicInteger();
        breaker.getEventPublisher().onSuccess(event -> successes.incrementAndGet());
        CompletableFuture<String> backend = new CompletableFuture<>();
        Future<String> old = CircuitBreaker.decorateFuture(breaker, () -> backend).get();
        completeHealthyPhase(breaker, clock);
        assertThat(breaker.getHalfOpenPhase().getPermittedTrafficPercentage()).isEqualTo(100);
        backend.complete("old success");
        assertThat(old.get()).isEqualTo("old success");
        assertThat(old.get(1, TimeUnit.SECONDS)).isEqualTo("old success");
        assertThat(successes.get()).isEqualTo(3);
        assertThat(breaker.getMetrics().getNumberOfBufferedCalls()).isZero();

        Future<String> cancelled = CircuitBreaker.decorateFuture(breaker,
            () -> new CompletableFuture<String>()).get();
        assertThat(cancelled.cancel(false)).isTrue();
        assertThatThrownBy(cancelled::get).isInstanceOf(CancellationException.class);
        assertThatThrownBy(() -> cancelled.get(1, TimeUnit.SECONDS))
            .isInstanceOf(CancellationException.class);
        assertThat(breaker.getMetrics().getNumberOfBufferedCalls()).isZero();
        assertThat(successes.get()).isEqualTo(3);
    }

    @Test
    void throwingClassifiersDoNotClassifyOrRecordTheInvocationTwice() {
        IllegalStateException failure = new IllegalStateException("classifier");
        for (int classifier = 0; classifier < 3; classifier++) {
            int selected = classifier;
            AtomicInteger evaluations = new AtomicInteger();
            CircuitBreaker breaker = breaker(new MutableClock(), config -> {
                if (selected == 0) {
                    config.recordResult(result -> { evaluations.incrementAndGet(); throw failure; });
                } else if (selected == 1) {
                    config.ignoreException(error -> { evaluations.incrementAndGet(); throw failure; });
                } else {
                    config.recordException(error -> { evaluations.incrementAndGet(); throw failure; });
                }
            });
            enterRecovery(breaker);
            CallPermission permission = admit(breaker);
            if (selected == 0) {
                assertThatThrownBy(() -> permission.onResult(0, TimeUnit.NANOSECONDS, "result"))
                    .isSameAs(failure);
            } else {
                assertThatThrownBy(() -> permission.onError(0, TimeUnit.NANOSECONDS, failure))
                    .isSameAs(failure);
            }
            permission.onError(0, TimeUnit.NANOSECONDS, failure);
            permission.release();
            assertThat(evaluations.get()).isEqualTo(1);
            assertThat(breaker.getMetrics().getNumberOfBufferedCalls()).isZero();
        }
    }

    @Test
    void asynchronousClassifierFailureAlwaysSettlesTheDecoratedPromise() {
        IllegalArgumentException failure = new IllegalArgumentException("classifier");
        CircuitBreaker breaker = breaker(new MutableClock(), config ->
            config.recordResult(result -> { throw failure; }));
        CompletableFuture<String> result = breaker.executeCompletionStage(() ->
            CompletableFuture.completedFuture("success")).toCompletableFuture();
        assertThat(result).isCompletedExceptionally();
        assertThatThrownBy(result::join).isInstanceOf(CompletionException.class).hasCause(failure);
        assertThat(breaker.getMetrics().getNumberOfBufferedCalls()).isZero();
    }

    @Test
    void cancelledAsynchronousOutcomeCanBeIgnoredWithoutSupplyingHealth() {
        CircuitBreaker breaker = breaker(new MutableClock(), config ->
            config.ignoreExceptions(CancellationException.class));
        CompletableFuture<String> source = new CompletableFuture<>();
        CompletableFuture<String> result = breaker.executeCompletionStage(() -> source).toCompletableFuture();
        source.cancel(false);
        assertThat(result).isCompletedExceptionally();
        assertThat(breaker.getMetrics().getNumberOfBufferedCalls()).isZero();
    }

    private static CircuitBreaker breaker(MutableClock clock,
        Consumer<CircuitBreakerConfig.Builder> customize) {
        CircuitBreakerConfig.Builder builder = CircuitBreakerConfig.custom().clock(clock)
            .slowCallDurationThreshold(Duration.ofMinutes(5)).halfOpenRecoveryConfig(recovery().build());
        customize.accept(builder);
        return CircuitBreaker.of("contextual", builder.build());
    }

    private static HalfOpenRecoveryConfig.Builder recovery() {
        return HalfOpenRecoveryConfig.custom().trafficPercentageStep(50)
            .minimumNumberOfCalls(2).minimumPhaseDuration(Duration.ofSeconds(10));
    }

    private static void enterRecovery(CircuitBreaker breaker) {
        breaker.transitionToOpenState();
        breaker.transitionToHalfOpenState();
    }

    private static CallPermission admit(CircuitBreaker breaker) {
        for (int attempt = 0; attempt < 100; attempt++) {
            CallPermission permission = breaker.tryAcquireCallPermission();
            if (permission != null) {
                return permission;
            }
        }
        throw new AssertionError("Expected an admitted slot");
    }

    private static void completeHealthyPhase(CircuitBreaker breaker, MutableClock clock) {
        CallPermission first = admit(breaker);
        CallPermission second = admit(breaker);
        first.onSuccess(0, TimeUnit.NANOSECONDS);
        clock.advance();
        second.onSuccess(0, TimeUnit.NANOSECONDS);
    }

    private static final class MutableClock extends Clock {
        private Instant now = Instant.EPOCH;
        private void advance() { now = now.plusSeconds(10); }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }
}
