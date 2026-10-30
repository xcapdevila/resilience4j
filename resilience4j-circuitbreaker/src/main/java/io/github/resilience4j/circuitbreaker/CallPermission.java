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

import io.github.resilience4j.core.lang.Nullable;

import java.util.concurrent.TimeUnit;

/**
 * An admitted invocation's completion handle. Native breakers configured with phased
 * recovery bind the handle to the exact state and phase visit which granted admission.
 * A late outcome retains ordinary call-event publication rules, but cannot supply
 * health samples to a later phase visit or transition a replacement state.
 * <p>
 * Report one outcome, or release an unused permission. Native phased handles accept
 * only the first terminal notification; subsequent notifications have no effect, even
 * when a classification predicate threw while processing the first notification.
 * Standard native breakers may reuse a forwarding handle, and standard recovery and
 * custom implementations retain their existing outcome semantics.
 *
 * @see CircuitBreaker#acquireCallPermission()
 * @see CircuitBreaker#tryAcquireCallPermission()
 */
public interface CallPermission {

    /**
     * Records this invocation's successful completion.
     *
     * @param duration the elapsed execution duration
     * @param unit the unit of the duration
     */
    void onSuccess(long duration, TimeUnit unit);

    /**
     * Classifies and records this invocation's exceptional completion. Ignored
     * exceptions release the permission without supplying a health sample. Exceptions
     * thrown by classification predicates propagate to the caller.
     *
     * @param duration the elapsed execution duration
     * @param unit the unit of the duration
     * @param error the invocation's failure
     */
    void onError(long duration, TimeUnit unit, Throwable error);

    /**
     * Classifies and records this invocation's returned result. Exceptions thrown by
     * classification predicates propagate to the caller without supplying a health sample.
     *
     * @param duration the elapsed execution duration
     * @param unit the unit of the duration
     * @param result the returned value, which may be null
     */
    void onResult(long duration, TimeUnit unit, @Nullable Object result);

    /**
     * Returns an unused or cancelled permission without providing a health sample.
     * For native phased handles, this consumes the handle's terminal notification.
     */
    void release();
}
