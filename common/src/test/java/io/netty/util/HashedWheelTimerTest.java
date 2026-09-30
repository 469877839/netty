/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.util;

import io.netty.util.concurrent.ImmediateExecutor;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class HashedWheelTimerTest {

    @Test
    public void testScheduleTimeoutShouldNotRunBeforeDelay() throws InterruptedException {
        final Timer timer = new HashedWheelTimer();
        final CountDownLatch barrier = new CountDownLatch(1);
        final Timeout timeout = timer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                fail("This should not have run");
                barrier.countDown();
            }
        }, 10, TimeUnit.SECONDS);
        assertFalse(barrier.await(3, TimeUnit.SECONDS));
        assertFalse(timeout.isExpired(), "timer should not expire");
        timer.stop();
    }

    @Test
    public void testScheduleTimeoutShouldRunAfterDelay() throws InterruptedException {
        final Timer timer = new HashedWheelTimer();
        final CountDownLatch barrier = new CountDownLatch(1);
        final Timeout timeout = timer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                barrier.countDown();
            }
        }, 2, TimeUnit.SECONDS);
        assertTrue(barrier.await(3, TimeUnit.SECONDS));
        assertTrue(timeout.isExpired(), "timer should expire");
        timer.stop();
    }

    @Test
    @org.junit.jupiter.api.Timeout(value = 3000, unit = TimeUnit.MILLISECONDS)
    public void testStopTimer() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(3);
        final Timer timerProcessed = new HashedWheelTimer();
        for (int i = 0; i < 3; i ++) {
            timerProcessed.newTimeout(new TimerTask() {
                @Override
                public void run(final Timeout timeout) throws Exception {
                    latch.countDown();
                }
            }, 1, TimeUnit.MILLISECONDS);
        }

        latch.await();
        assertEquals(0, timerProcessed.stop().size(), "Number of unprocessed timeouts should be 0");

        final Timer timerUnprocessed = new HashedWheelTimer();
        for (int i = 0; i < 5; i ++) {
            timerUnprocessed.newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {
                }
            }, 5, TimeUnit.SECONDS);
        }
        Thread.sleep(1000L); // sleep for a second
        assertFalse(timerUnprocessed.stop().isEmpty(), "Number of unprocessed timeouts should be greater than 0");
    }

    @Test
    @org.junit.jupiter.api.Timeout(value = 3000, unit = TimeUnit.MILLISECONDS)
    public void testTimerShouldThrowExceptionAfterShutdownForNewTimeouts() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(3);
        final Timer timer = new HashedWheelTimer();
        for (int i = 0; i < 3; i ++) {
            timer.newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {
                    latch.countDown();
                }
            }, 1, TimeUnit.MILLISECONDS);
        }

        latch.await();
        timer.stop();

        try {
            timer.newTimeout(createNoOpTimerTask(), 1, TimeUnit.MILLISECONDS);
            fail("Expected exception didn't occur.");
        } catch (IllegalStateException ignored) {
            // expected
        }
    }

    @Test
    @org.junit.jupiter.api.Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    public void testTimerOverflowWheelLength() throws InterruptedException {
        final HashedWheelTimer timer = new HashedWheelTimer(
            Executors.defaultThreadFactory(), 100, TimeUnit.MILLISECONDS, 32);
        final CountDownLatch latch = new CountDownLatch(3);

        timer.newTimeout(new TimerTask() {
            @Override
            public void run(final Timeout timeout) throws Exception {
                timer.newTimeout(this, 100, TimeUnit.MILLISECONDS);
                latch.countDown();
            }
        }, 100, TimeUnit.MILLISECONDS);

        latch.await();
        assertFalse(timer.stop().isEmpty());
    }

    @Test
    public void testExecutionOnTime() throws InterruptedException {
        int tickDuration = 200;
        int timeout = 125;
        int maxTimeout = 2 * (tickDuration + timeout);
        final HashedWheelTimer timer = new HashedWheelTimer(tickDuration, TimeUnit.MILLISECONDS);
        final BlockingQueue<Long> queue = new LinkedBlockingQueue<Long>();

        int scheduledTasks = 100000;
        for (int i = 0; i < scheduledTasks; i++) {
            final long start = System.nanoTime();
            timer.newTimeout(new TimerTask() {
                @Override
                public void run(final Timeout timeout) throws Exception {
                    queue.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
                }
            }, timeout, TimeUnit.MILLISECONDS);
        }

        for (int i = 0; i < scheduledTasks; i++) {
            long delay = queue.take();
            assertTrue(delay >= timeout && delay < maxTimeout,
                "Timeout + " + scheduledTasks + " delay " + delay + " must be " + timeout + " < " + maxTimeout);
        }

        timer.stop();
    }

    @Test
    public void testExecutionOnTaskExecutor() throws InterruptedException {
        int timeout = 10;

        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch timeoutLatch = new CountDownLatch(1);
        Executor executor = new Executor() {
            @Override
            public void execute(Runnable command) {
                try {
                    command.run();
                } finally {
                    latch.countDown();
                }
            }
        };
        final HashedWheelTimer timer = new HashedWheelTimer(Executors.defaultThreadFactory(), 100,
                TimeUnit.MILLISECONDS, 32, true, 2, executor);
        timer.newTimeout(new TimerTask() {
            @Override
            public void run(final Timeout timeout) throws Exception {
                timeoutLatch.countDown();
            }
        }, timeout, TimeUnit.MILLISECONDS);

        latch.await();
        timeoutLatch.await();
        timer.stop();
    }

    @Test
    public void testRejectedExecutionExceptionWhenTooManyTimeoutsAreAddedBackToBack() {
        HashedWheelTimer timer = new HashedWheelTimer(Executors.defaultThreadFactory(), 100,
            TimeUnit.MILLISECONDS, 32, true, 2);
        timer.newTimeout(createNoOpTimerTask(), 5, TimeUnit.SECONDS);
        timer.newTimeout(createNoOpTimerTask(), 5, TimeUnit.SECONDS);
        try {
            timer.newTimeout(createNoOpTimerTask(), 1, TimeUnit.MILLISECONDS);
            fail("Timer allowed adding 3 timeouts when maxPendingTimeouts was 2");
        } catch (RejectedExecutionException e) {
            // Expected
        } finally {
            timer.stop();
        }
    }

    @Test
    public void testNewTimeoutShouldStopThrowingRejectedExecutionExceptionWhenExistingTimeoutIsCancelled()
        throws InterruptedException {
        final int tickDurationMs = 100;
        final HashedWheelTimer timer = new HashedWheelTimer(Executors.defaultThreadFactory(), tickDurationMs,
            TimeUnit.MILLISECONDS, 32, true, 2);
        timer.newTimeout(createNoOpTimerTask(), 5, TimeUnit.SECONDS);
        Timeout timeoutToCancel = timer.newTimeout(createNoOpTimerTask(), 5, TimeUnit.SECONDS);
        assertTrue(timeoutToCancel.cancel());

        Thread.sleep(tickDurationMs * 5);

        final CountDownLatch secondLatch = new CountDownLatch(1);
        timer.newTimeout(createCountDownLatchTimerTask(secondLatch), 90, TimeUnit.MILLISECONDS);

        secondLatch.await();
        timer.stop();
    }

    @Test
    @org.junit.jupiter.api.Timeout(value = 3000, unit = TimeUnit.MILLISECONDS)
    public void testNewTimeoutShouldStopThrowingRejectedExecutionExceptionWhenExistingTimeoutIsExecuted()
        throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final HashedWheelTimer timer = new HashedWheelTimer(Executors.defaultThreadFactory(), 25,
            TimeUnit.MILLISECONDS, 4, true, 2);
        timer.newTimeout(createNoOpTimerTask(), 5, TimeUnit.SECONDS);
        timer.newTimeout(createCountDownLatchTimerTask(latch), 90, TimeUnit.MILLISECONDS);

        latch.await();

        final CountDownLatch secondLatch = new CountDownLatch(1);
        timer.newTimeout(createCountDownLatchTimerTask(secondLatch), 90, TimeUnit.MILLISECONDS);

        secondLatch.await();
        timer.stop();
    }

    @Test()
    public void reportPendingTimeouts() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final HashedWheelTimer timer = new HashedWheelTimer();
        final Timeout t1 = timer.newTimeout(createNoOpTimerTask(), 100, TimeUnit.MINUTES);
        final Timeout t2 = timer.newTimeout(createNoOpTimerTask(), 100, TimeUnit.MINUTES);
        timer.newTimeout(createCountDownLatchTimerTask(latch), 90, TimeUnit.MILLISECONDS);

        assertEquals(3, timer.pendingTimeouts());
        t1.cancel();
        t2.cancel();
        latch.await();

        assertEquals(0, timer.pendingTimeouts());
        timer.stop();
    }

    @Test
    public void testOverflow() throws InterruptedException  {
        final HashedWheelTimer timer = new HashedWheelTimer();
        final CountDownLatch latch = new CountDownLatch(1);
        Timeout timeout = timer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) {
                latch.countDown();
            }
        }, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        assertFalse(latch.await(1, TimeUnit.SECONDS));
        timeout.cancel();
        timer.stop();
    }

    @Test
    @org.junit.jupiter.api.Timeout(value = 3000, unit = TimeUnit.MILLISECONDS)
    public void testStopTimerCancelsPendingTasks() throws InterruptedException {
        final Timer timerUnprocessed = new HashedWheelTimer();
        for (int i = 0; i < 5; i ++) {
            timerUnprocessed.newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {
                }
            }, 5, TimeUnit.SECONDS);
        }
        Thread.sleep(1000L); // sleep for a second

        for (Timeout timeout : timerUnprocessed.stop()) {
            assertTrue(timeout.isCancelled(), "All unprocessed tasks should be canceled");
        }
    }

    @org.junit.jupiter.api.Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    public void cancelWillCallCallback() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final HashedWheelTimer timer = new HashedWheelTimer();
        final Timeout t1 = timer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) {
                fail();
            }

            @Override
            public void cancelled(Timeout timeout) {
                latch.countDown();
            }
        }, 90, TimeUnit.MILLISECONDS);

        assertEquals(1, timer.pendingTimeouts());
        t1.cancel();
        latch.await();
    }

    @Test
    public void testPendingTimeoutsShouldBeCountedCorrectlyWhenTimeoutCancelledWithinGoalTick()
        throws InterruptedException {
        final HashedWheelTimer timer = new HashedWheelTimer();
        final CountDownLatch barrier = new CountDownLatch(1);
        // A total of 11 timeouts with the same delay are submitted, and they will be processed in the same tick.
        timer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                barrier.countDown();
                Thread.sleep(1000);
            }
        }, 200, TimeUnit.MILLISECONDS);
        List<Timeout> timeouts = new ArrayList<Timeout>();
        for (int i = 0; i < 10; i++) {
            timeouts.add(timer.newTimeout(createNoOpTimerTask(), 200, TimeUnit.MILLISECONDS));
        }
        barrier.await();
        // The simulation here is that the timeout has been transferred to a bucket and is canceled before it is
        // actually expired in the goal tick.
        for (Timeout timeout : timeouts) {
            timeout.cancel();
        }
        Thread.sleep(2000);
        assertEquals(0, timer.pendingTimeouts());
        timer.stop();
    }

    private static HashedWheelTimer newIsolatedTimer(long tickDurationMs, int ticksPerWheel,
                                                    long maxPendingTimeouts) {
        return new HashedWheelTimer(Executors.defaultThreadFactory(), tickDurationMs,
                TimeUnit.MILLISECONDS, ticksPerWheel, true, maxPendingTimeouts,
                ImmediateExecutor.INSTANCE, true);
    }

    private static HashedWheelTimer newIsolatedTimer() {
        return newIsolatedTimer(10, 512, -1);
    }

    @Test
    @org.junit.jupiter.api.Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    public void testIsolationDisabledByDefaultRunsOnWorkerThread() throws InterruptedException {
        final String tag = "-default-" + java.util.UUID.randomUUID();
        final ThreadFactory factory = new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = Executors.defaultThreadFactory().newThread(r);
                thread.setName(thread.getName() + tag);
                return thread;
            }
        };
        // All old constructors must behave exactly as before: no isolated executor threads and
        // the task runs inline on the single worker thread.
        final HashedWheelTimer timer = new HashedWheelTimer(factory, 10, TimeUnit.MILLISECONDS, 512);
        final CountDownLatch done = new CountDownLatch(1);
        final AtomicReference<String> threadName = new AtomicReference<String>();
        timer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) {
                threadName.set(Thread.currentThread().getName());
                done.countDown();
            }
        }, 50, TimeUnit.MILLISECONDS);
        assertTrue(done.await(2, TimeUnit.SECONDS));
        assertTrue(threadName.get().contains(tag) && !threadName.get().contains("isolatedTimeout"),
                "task should run inline on the worker thread, was: " + threadName.get());
        timer.stop();
    }

    @Test
    @org.junit.jupiter.api.Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    public void testIsolatedModeLongRunningTaskDoesNotDelayLaterBatches() throws InterruptedException {
        final HashedWheelTimer timer = newIsolatedTimer();
        try {
            final CountDownLatch slowStarted = new CountDownLatch(1);
            final CountDownLatch slowRelease = new CountDownLatch(1);
            final CountDownLatch fastDone = new CountDownLatch(3);

            timer.newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws InterruptedException {
                    slowStarted.countDown();
                    slowRelease.await(5, TimeUnit.SECONDS);
                }
            }, 50, TimeUnit.MILLISECONDS);
            assertTrue(slowStarted.await(2, TimeUnit.SECONDS));

            // Tasks of later expiration ticks must start promptly even though the first task
            // is still sleeping for ~500ms.
            final long[] starts = new long[3];
            for (int i = 0; i < starts.length; i++) {
                final int index = i;
                final long scheduledAt = System.nanoTime();
                starts[i] = scheduledAt;
                timer.newTimeout(new TimerTask() {
                    @Override
                    public void run(Timeout timeout) {
                        starts[index] = System.nanoTime() - scheduledAt;
                        fastDone.countDown();
                    }
                }, 50, TimeUnit.MILLISECONDS);
            }

            assertTrue(fastDone.await(2, TimeUnit.SECONDS), "later tasks should not be blocked");
            for (long elapsed : starts) {
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(elapsed);
                assertTrue(elapsedMs <= 200,
                        "task started " + elapsedMs + "ms after its deadline, expected <= 200ms");
            }
            slowRelease.countDown();
        } finally {
            timer.stop();
        }
    }

    @Test
    @org.junit.jupiter.api.Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    public void testIsolatedModeCancelledQueuedTaskNeverRuns() throws InterruptedException {
        // Saturate the bounded pool (8 threads) so the 9th task of the batch is queued.
        final HashedWheelTimer timer = newIsolatedTimer();
        try {
            final CountDownLatch blockersStarted = new CountDownLatch(8);
            final CountDownLatch release = new CountDownLatch(1);
            final AtomicBoolean victimRan = new AtomicBoolean();

            for (int i = 0; i < 8; i++) {
                timer.newTimeout(new TimerTask() {
                    @Override
                    public void run(Timeout timeout) throws InterruptedException {
                        blockersStarted.countDown();
                        release.await(5, TimeUnit.SECONDS);
                    }
                }, 50, TimeUnit.MILLISECONDS);
            }
            assertTrue(blockersStarted.await(2, TimeUnit.SECONDS));

            Timeout victim = timer.newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) {
                    victimRan.set(true);
                }
            }, 50, TimeUnit.MILLISECONDS);

            // The victim is dispatched (and queued in the saturated executor) once its tick
            // expires, but must not start.
            Thread.sleep(150);
            assertTrue(victim.cancel());
            release.countDown();
            Thread.sleep(100);
            assertFalse(victimRan.get(), "cancelled task must not run even if already dispatched");
        } finally {
            timer.stop();
        }
    }

    @Test
    @org.junit.jupiter.api.Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    public void testIsolatedModeCancelAfterTaskStartedReturnsFalse() throws InterruptedException {
        final HashedWheelTimer timer = newIsolatedTimer();
        try {
            final CountDownLatch started = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final AtomicBoolean cancelledObserved = new AtomicBoolean(false);

            Timeout timeout = timer.newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws InterruptedException {
                    started.countDown();
                    release.await(5, TimeUnit.SECONDS);
                }

                @Override
                public void cancelled(Timeout timeout) {
                    cancelledObserved.set(true);
                }
            }, 50, TimeUnit.MILLISECONDS);

            assertTrue(started.await(2, TimeUnit.SECONDS));
            assertFalse(timeout.cancel());
            release.countDown();
            assertFalse(cancelledObserved.get(), "cancelled callback must not fire once running");
        } finally {
            timer.stop();
        }
    }

    @Test
    @org.junit.jupiter.api.Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    public void testIsolatedModeStopBoundedAndReturnsOnlyNotStartedTasks() throws InterruptedException {
        final HashedWheelTimer timer = newIsolatedTimer();
        final CountDownLatch runningStarted = new CountDownLatch(4);
        final CountDownLatch release = new CountDownLatch(1);

        // Occupy all 8 pool threads with stuck tasks, then queue 4 more tasks that must not start.
        for (int i = 0; i < 8; i++) {
            final boolean countDown = i < 4;
            timer.newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws InterruptedException {
                    if (countDown) {
                        runningStarted.countDown();
                    }
                    try {
                        release.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        // stop() interrupts running tasks; keep blocking anyway to prove stop()
                        // does not wait for us.
                        Thread.sleep(20000);
                    }
                }
            }, 50, TimeUnit.MILLISECONDS);
        }
        for (int i = 0; i < 4; i++) {
            timer.newTimeout(createNoOpTimerTask(), 50, TimeUnit.MILLISECONDS);
        }
        assertTrue(runningStarted.await(2, TimeUnit.SECONDS));
        // Let the worker dispatch the queued tasks into the executor queue before stopping.
        Thread.sleep(150);

        long begin = System.nanoTime();
        Set<Timeout> unprocessed = timer.stop();
        long stopMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

        assertTrue(stopMillis <= 1000, "stop() took " + stopMillis + "ms despite stuck tasks");
        assertEquals(4, unprocessed.size(), "only not-started tasks must be returned");
        for (Timeout timeout : unprocessed) {
            assertTrue(timeout.isCancelled());
        }
        release.countDown();
    }

    @Test
    @org.junit.jupiter.api.Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    public void testIsolatedModeStopNotifiesCancelledCallbackOfQueuedTasks() throws InterruptedException {
        final HashedWheelTimer timer = newIsolatedTimer();
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch callbacks = new CountDownLatch(4);

        for (int i = 0; i < 8; i++) {
            timer.newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws InterruptedException {
                    release.await(10, TimeUnit.SECONDS);
                }
            }, 50, TimeUnit.MILLISECONDS);
        }
        for (int i = 0; i < 4; i++) {
            timer.newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) {
                }

                @Override
                public void cancelled(Timeout timeout) {
                    callbacks.countDown();
                }
            }, 50, TimeUnit.MILLISECONDS);
        }
        Thread.sleep(150);

        timer.stop();
        assertTrue(callbacks.await(1, TimeUnit.SECONDS),
                "cancelled callback must fire for queued tasks during stop()");
        release.countDown();
    }

    @Test
    @org.junit.jupiter.api.Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    public void testIsolatedModeThreadCountIsCapped() throws InterruptedException {
        final String tag = "-isolatedTimeout-" + java.util.UUID.randomUUID();
        final ThreadFactory countingFactory = new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = Executors.defaultThreadFactory().newThread(r);
                thread.setName(thread.getName() + tag);
                return thread;
            }
        };
        // The isolation factory appends "-isolatedTimeout", so look for the unique tag prefix.
        final HashedWheelTimer timer = new HashedWheelTimer(countingFactory, 10, TimeUnit.MILLISECONDS,
                512, true, -1, ImmediateExecutor.INSTANCE, true);
        try {
            final CountDownLatch started = new CountDownLatch(8);
            final CountDownLatch release = new CountDownLatch(1);
            for (int i = 0; i < 20; i++) {
                timer.newTimeout(new TimerTask() {
                    @Override
                    public void run(Timeout timeout) throws InterruptedException {
                        started.countDown();
                        release.await(5, TimeUnit.SECONDS);
                    }
                }, 50, TimeUnit.MILLISECONDS);
            }
            // Only the first 8 dispatched tasks start; the other 12 stay queued.
            assertTrue(started.await(1, TimeUnit.SECONDS));
            Thread.sleep(150);
            int isolatedThreads = 0;
            for (Thread thread : Thread.getAllStackTraces().keySet()) {
                if (thread.getName().contains(tag) && thread.getName().endsWith("-isolatedTimeout")) {
                    isolatedThreads++;
                }
            }
            assertTrue(isolatedThreads <= 8, "at most 8 isolated threads, saw " + isolatedThreads);
            assertEquals(8, isolatedThreads);
            release.countDown();
        } finally {
            timer.stop();
        }
    }

    @Test
    @org.junit.jupiter.api.Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    public void testIsolatedModeSameDeadlineTasksRunConcurrently() throws InterruptedException {
        // Documents the chosen tradeoff: isolation (parallel start) is preferred over the
        // cross-thread FIFO start order for same-deadline tasks.
        final HashedWheelTimer timer = newIsolatedTimer();
        try {
            final CountDownLatch bothInside = new CountDownLatch(2);
            final CountDownLatch release = new CountDownLatch(1);
            for (int i = 0; i < 2; i++) {
                timer.newTimeout(new TimerTask() {
                    @Override
                    public void run(Timeout timeout) throws InterruptedException {
                        bothInside.countDown();
                        release.await(5, TimeUnit.SECONDS);
                    }
                }, 50, TimeUnit.MILLISECONDS);
            }
            assertTrue(bothInside.await(1, TimeUnit.SECONDS),
                    "same-deadline tasks should run in parallel");
            release.countDown();
        } finally {
            timer.stop();
        }
    }

    @Test
    @org.junit.jupiter.api.Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    public void testIsolatedModePendingCountAndLimitUnchanged() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final HashedWheelTimer timer = newIsolatedTimer(10, 512, 2);
        try {
            Timeout longTask = timer.newTimeout(createNoOpTimerTask(), 5, TimeUnit.SECONDS);
            timer.newTimeout(createCountDownLatchTimerTask(latch), 90, TimeUnit.MILLISECONDS);
            try {
                timer.newTimeout(createNoOpTimerTask(), 1, TimeUnit.MILLISECONDS);
                fail("timer should reject above maxPendingTimeouts");
            } catch (RejectedExecutionException expected) {
                // expected
            }
            // Releasing the slot (cancel the long task) must lift the rejection.
            assertTrue(longTask.cancel());
            long pendingDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (timer.pendingTimeouts() != 1 && System.nanoTime() < pendingDeadline) {
                Thread.sleep(10);
            }
            final CountDownLatch second = new CountDownLatch(1);
            timer.newTimeout(createCountDownLatchTimerTask(second), 50, TimeUnit.MILLISECONDS);
            latch.await();
            assertTrue(second.await(2, TimeUnit.SECONDS));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (timer.pendingTimeouts() != 0 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(0, timer.pendingTimeouts());
        } finally {
            timer.stop();
        }
    }

    private static TimerTask createNoOpTimerTask() {
        return new TimerTask() {
            @Override
            public void run(final Timeout timeout) throws Exception {
            }
        };
    }

    private static TimerTask createCountDownLatchTimerTask(final CountDownLatch latch) {
        return new TimerTask() {
            @Override
            public void run(final Timeout timeout) throws Exception {
                latch.countDown();
            }
        };
    }
}
