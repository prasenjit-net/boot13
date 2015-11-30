package com.example.boot13;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.cluster.leader.Candidate;
import org.springframework.cloud.cluster.leader.Context;
import org.springframework.cloud.cluster.leader.event.LeaderEventPublisher;
import org.springframework.context.Lifecycle;
import org.springframework.util.Assert;

import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;

/**
 * Bootstrap leadership {@link org.springframework.cloud.cluster.leader.Candidate candidates}
 * with Hazelcast. Upon construction, {@link #start} must be invoked to
 * register the candidate for leadership election.
 *
 * @author Patrick Peralta
 */
public class LeaderInitiator implements Lifecycle, InitializingBean, DisposableBean {

    @Autowired
    private final Cache client;

    /**
     * Candidate for leader election.
     */
    private final Candidate candidate;

    private volatile boolean leader = false;

    /**
     * Executor service for running leadership daemon.
     */
    private final ExecutorService executorService = Executors.newSingleThreadExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, "Hazelcast leadership");
            thread.setDaemon(true);
            return thread;
        }
    });

    /**
     * Future returned by submitting an {@link Initiator} to {@link #executorService}.
     * This is used to cancel leadership.
     */
    private volatile Future<Void> future;

    /**
     * Hazelcast distributed map used for locks.
     */
    private volatile Region<String, String> mapLocks;

    /**
     * Flag that indicates whether the leadership election for
     * this {@link #candidate} is running.
     */
    private volatile boolean running;

    /**
     * Leader event publisher if set
     */
    private LeaderEventPublisher leaderEventPublisher;

    /**
     * Construct a {@link LeaderInitiator}.
     *
     * @param client    Hazelcast client
     * @param candidate leadership election candidate
     */
    public LeaderInitiator(Cache client, Candidate candidate) {
        this.client = client;
        this.candidate = candidate;
    }

    /**
     * Start the registration of the {@link #candidate} for leader election.
     */
    @Override
    public synchronized void start() {
        if (!running) {
            mapLocks = client.<String, String>createRegionFactory(RegionShortcut.PARTITION).create("cluster");
            running = true;
            future = executorService.submit(new Initiator());
        }
    }

    /**
     * Stop the registration of the {@link #candidate} for leader election.
     * If the candidate is currently leader, its leadership will be revoked.
     */
    @Override
    public synchronized void stop() {
        if (running) {
            running = false;
            future.cancel(true);
        }
    }

    /**
     * @return true if leadership election for this {@link #candidate} is running
     */
    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        start();
    }

    @Override
    public void destroy() throws Exception {
        stop();
        executorService.shutdown();
    }

    /**
     * Sets the {@link LeaderEventPublisher}.
     *
     * @param leaderEventPublisher the event publisher
     */
    public void setLeaderEventPublisher(LeaderEventPublisher leaderEventPublisher) {
        this.leaderEventPublisher = leaderEventPublisher;
    }

    /**
     * Callable that manages the acquisition of Hazelcast locks
     * for leadership election.
     */
    class Initiator implements Callable<Void> {

        @Override
        public Void call() throws Exception {
            Assert.state(mapLocks != null);
            GemfireContext context = new GemfireContext();
            String role = candidate.getRole();
            Lock locked = null;

            while (running) {
                try {
                    locked = mapLocks.getRegionDistributedLock();
                    locked.lockInterruptibly();
                    mapLocks.put(role, candidate.getId());
                    leader = true;
                    candidate.onGranted(context);
                    if (leaderEventPublisher != null) {
                        leaderEventPublisher.publishOnGranted(LeaderInitiator.this, context);
                    }
                    Thread.sleep(Long.MAX_VALUE);

                } catch (Exception e) {
                    System.out.println(e);
                    // InterruptedException, like any other runtime exception,
                    // is handled by the finally block below. No need to
                    // reset the interrupt flag as the interrupt is handled.
                } finally {
                    if (locked != null) {
                        mapLocks.remove(role);
                        locked.unlock();
                        leader = false;
                        candidate.onRevoked(context);
                        if (leaderEventPublisher != null) {
                            leaderEventPublisher.publishOnRevoked(LeaderInitiator.this, context);
                        }
                        locked = null;
                    }
                }
            }
            return null;
        }

    }

    /**
     * Implementation of leadership context backed by Hazelcast.
     */
    class GemfireContext implements Context {

        @Override
        public boolean isLeader() {
            return leader;
        }

        @Override
        public void yield() {
            if (future != null) {
                future.cancel(true);
            }
        }

        @Override
        public String toString() {
            return String.format("HazelcastContext{role=%s, id=%s, isLeader=%s}",
                    candidate.getRole(), candidate.getId(), isLeader());
        }
    }
}
