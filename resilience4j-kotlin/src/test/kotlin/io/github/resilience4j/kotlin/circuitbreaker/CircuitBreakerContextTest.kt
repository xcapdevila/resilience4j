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
package io.github.resilience4j.kotlin.circuitbreaker

import io.github.resilience4j.circuitbreaker.CallPermission
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.HalfOpenRecoveryConfig
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class CircuitBreakerContextTest {
    @Test
    fun `suspend completion belongs to the phase which admitted it`() = runBlocking {
        val fixture = Fixture()
        val result = fixture.breaker.executeSuspendFunction {
            fixture.advanceToHundredPercent()
            "late"
        }

        assertThat(result).isEqualTo("late")
        fixture.assertCurrentWindowEmpty()
        fixture.validateCurrentPhase()
    }

    @Test
    fun `flow completion belongs to the phase which admitted its collection`() = runBlocking {
        val fixture = Fixture()
        val values = flow {
            fixture.advanceToHundredPercent()
            emit("late")
        }.circuitBreaker(fixture.breaker).toList()

        assertThat(values).containsExactly("late")
        fixture.assertCurrentWindowEmpty()
        fixture.validateCurrentPhase()
    }

    @Test
    fun `suspend cancellation releases its context without a health sample`() {
        val fixture = Fixture()
        assertThatThrownBy {
            runBlocking {
                fixture.breaker.executeSuspendFunction<String> {
                    throw CancellationException("cancelled")
                }
            }
        }.isInstanceOf(CancellationException::class.java)

        assertThat(fixture.breaker.metrics.numberOfBufferedCalls).isZero()
        fixture.advanceToHundredPercent()
        fixture.validateCurrentPhase()
    }

    @Test
    fun `flow cancellation releases its context without a health sample`() {
        val fixture = Fixture()
        assertThatThrownBy {
            runBlocking {
                flow<String> { throw CancellationException("cancelled") }
                    .circuitBreaker(fixture.breaker).toList()
            }
        }.isInstanceOf(CancellationException::class.java)

        assertThat(fixture.breaker.metrics.numberOfBufferedCalls).isZero()
        fixture.advanceToHundredPercent()
        fixture.validateCurrentPhase()
    }

    private class Fixture {
        val clock = MutableClock()
        val breaker: CircuitBreaker = CircuitBreaker.of("contextual-kotlin",
            CircuitBreakerConfig.custom().clock(clock).initialState(CircuitBreaker.State.HALF_OPEN)
                .halfOpenRecoveryConfig(HalfOpenRecoveryConfig.custom()
                    .initialTrafficPercentage(75).trafficPercentageStep(25)
                    .minimumPhaseDuration(DWELL).minimumNumberOfCalls(1).build()).build())

        fun advanceToHundredPercent() {
            var permission: CallPermission? = null
            for (attempt in 0 until 100) {
                permission = breaker.tryAcquireCallPermission()
                if (permission != null) break
            }
            assertThat(permission).isNotNull()
            clock.advance(DWELL)
            permission!!.onSuccess(0, TimeUnit.NANOSECONDS)
            assertCurrentWindowEmpty()
        }

        fun assertCurrentWindowEmpty() {
            assertThat(breaker.state).isEqualTo(CircuitBreaker.State.HALF_OPEN)
            assertThat(breaker.halfOpenPhase.permittedTrafficPercentage).isEqualTo(100)
            assertThat(breaker.metrics.numberOfBufferedCalls).isZero()
        }

        fun validateCurrentPhase() {
            breaker.executeSupplier {
                clock.advance(DWELL)
                "current"
            }
            assertThat(breaker.state).isEqualTo(CircuitBreaker.State.CLOSED)
        }
    }

    private class MutableClock(
        private val now: AtomicReference<Instant> = AtomicReference(Instant.parse("2026-01-01T00:00:00Z")),
        private val clockZone: ZoneId = ZoneOffset.UTC
    ) : Clock() {
        fun advance(duration: Duration) {
            now.updateAndGet { it.plus(duration) }
        }

        override fun getZone(): ZoneId = clockZone
        override fun withZone(zone: ZoneId): Clock = MutableClock(now, zone)
        override fun instant(): Instant = now.get()
    }

    companion object {
        private val DWELL: Duration = Duration.ofSeconds(1)
    }
}
