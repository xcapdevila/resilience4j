/*
 * Copyright 2019 Robert Winkler
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.resilience4j.rxjava3.circuitbreaker.operator;

import io.github.resilience4j.circuitbreaker.CallPermission;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.rxjava3.AbstractSingleObserver;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.core.SingleObserver;
import io.reactivex.rxjava3.internal.disposables.EmptyDisposable;

import static io.github.resilience4j.circuitbreaker.CallNotPermittedException.createCallNotPermittedException;

class SingleCircuitBreaker<T> extends Single<T> {

    private final CircuitBreaker circuitBreaker;
    private final Single<T> upstream;

    SingleCircuitBreaker(Single<T> upstream, CircuitBreaker circuitBreaker) {
        this.upstream = upstream;
        this.circuitBreaker = circuitBreaker;
    }

    @Override
    protected void subscribeActual(SingleObserver<? super T> downstream) {
        CallPermission permission = circuitBreaker.tryAcquireCallPermission();
        if (permission != null) {
            upstream.subscribe(new CircuitBreakerSingleObserver(downstream, permission));
        } else {
            downstream.onSubscribe(EmptyDisposable.INSTANCE);
            downstream.onError(createCallNotPermittedException(circuitBreaker));
        }
    }

    class CircuitBreakerSingleObserver extends AbstractSingleObserver<T> {

        private final long start;
        private final CallPermission permission;

        CircuitBreakerSingleObserver(SingleObserver<? super T> downstreamObserver, CallPermission permission) {
            super(downstreamObserver);
            this.permission = permission;
            this.start = circuitBreaker.getCurrentTimestamp();
        }

        @Override
        protected void hookOnError(Throwable e) {
            permission.onError(circuitBreaker.getCurrentTimestamp() - start, circuitBreaker.getTimestampUnit(), e);
        }

        @Override
        protected void hookOnSuccess(T value) {
            permission.onResult(circuitBreaker.getCurrentTimestamp() - start, circuitBreaker.getTimestampUnit(), value);
        }

        @Override
        protected void hookOnCancel() {
            permission.release();
        }
    }
}
