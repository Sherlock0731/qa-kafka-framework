package qa.autotest.framework.utils;

import lombok.extern.slf4j.Slf4j;
import qa.autotest.framework.application.service.KafkaTestFacade;
import qa.autotest.framework.domain.model.ConsumeResult;
import qa.autotest.framework.domain.model.ConsumerGroup;
import qa.autotest.framework.domain.model.Message;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

/**
 * Awaitility-based helper for Hexagonal Architecture Kafka tests.
 * <p>
 * IMPORTANT: KafkaConsumer is ThreadLocal — subscribe/poll MUST run
 * in the same thread (main test thread). Therefore:
 * <p>
 * - Methods that call kafka.subscribe() / kafka.poll() use
 * pollInSameThread() so Awaitility never moves them to another thread.
 * <p>
 * - Pure infrastructure checks (topicExists) may run in any thread.
 * <p>
 * Usage:
 * <pre>
 *   KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);
 *   KafkaAwaitHelper.awaitPropagation(kafka, topicName, 10, 15);
 *   List&lt;Message&gt; msgs = KafkaAwaitHelper.awaitMessages(kafka, topicName, 10, 30);
 *   List&lt;Message&gt; msgs = KafkaAwaitHelper.awaitNewMessages(kafka, 10, 30);
 * </pre>
 */
@Slf4j
public class KafkaAwaitHelper {

    private KafkaAwaitHelper() {
    }

    /**
     * Poll interval for topic/infra checks (no consumer involved)
     */
    private static final Duration INFRA_INTERVAL = Duration.ofMillis(300);

    /**
     * Subscribes to a topic, then waits until the consumer has joined the
     * consumer group <strong>and</strong> the group coordinator has assigned
     * at least one partition to this consumer instance.
     *
     * <h3>Why poll() + isAssigned(), not just poll()?</h3>
     * The previous implementation used:
     * <pre>{@code
     * .until(() -> {
     *     ConsumeResult r = kafka.poll(Duration.ofMillis(300));
     *     return r != null;   // always true — poll() never returns null
     * });
     * }</pre>
     * {@code poll()} returns {@link ConsumeResult#empty()} or
     * {@link ConsumeResult#failureFrom} — never {@code null} — so the
     * condition evaluated to {@code true} on the very first iteration,
     * regardless of whether the group coordinator had finished the rebalance.
     * Tests calling {@code awaitConsumerReady()} then immediately polled for
     * messages and received zero results because the consumer was still in
     * {@code PREPARING_REBALANCE}.
     *
     * <h3>Correct termination condition</h3>
     * {@code poll()} is required to drive the JoinGroup / SyncGroup Kafka
     * protocol — without it, the broker never delivers the partition
     * assignment to the client. {@code isAssigned()} reads
     * {@code KafkaConsumer.assignment()}, which is a local in-memory set
     * that becomes non-empty only after the rebalance protocol completes and
     * the next {@code poll()} processes the assignment response. The
     * combination of {@code poll()} <em>then</em> {@code isAssigned()} is
     * therefore the minimal correct readiness check.
     *
     * <h3>Threading</h3>
     * {@code pollInSameThread()} ensures both calls execute on the calling
     * (main test) thread, which is mandatory because {@code KafkaConsumer}
     * is stored in a {@code ThreadLocal} and must never be accessed from
     * any other thread.
     *
     * @param kafka      facade
     * @param topicName  topic to subscribe to
     * @param timeoutSec max wait in seconds
     * @throws org.awaitility.core.ConditionTimeoutException if partition
     *         assignment does not complete within {@code timeoutSec}
     */
    public static void awaitConsumerReady(KafkaTestFacade kafka,
                                          String topicName,
                                          int timeoutSec) {
        log.debug("Subscribing and awaiting consumer ready: {}", topicName);
        kafka.subscribe(topicName);

        await("consumer group joined + partitions assigned: " + topicName)
                .atMost(timeoutSec, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(500))
                .pollInSameThread()
                .ignoreException(IllegalStateException.class)
                .until(() -> {
                    kafka.poll(Duration.ofMillis(300));
                    boolean assigned = kafka.isAssigned();
                    log.trace("awaitConsumerReady: isAssigned={} on '{}'", assigned, topicName);
                    return assigned;
                });

        log.debug("Consumer ready on topic '{}': partitions assigned", topicName);
    }

    /**
     * Flushes the producer, then waits until Kafka has confirmed the topic
     * is accessible (topic exists check — admin client, any thread is fine).
     * <p>
     * Replaces: flush() + Thread.sleep(N).
     *
     * @param kafka         facade
     * @param topicName     topic to check
     * @param expectedCount used only for logging context
     * @param timeoutSec    max wait in seconds
     */
    public static void awaitPropagation(KafkaTestFacade kafka,
                                        String topicName,
                                        int expectedCount,
                                        int timeoutSec) {
        log.debug("Flushing and awaiting propagation of ~{} messages to {}",
                expectedCount, topicName);
        kafka.flush();

        // topicExists uses AdminClient — thread-safe, no ThreadLocal issue
        await("topic accessible: " + topicName)
                .atMost(timeoutSec, TimeUnit.SECONDS)
                .pollInterval(INFRA_INTERVAL)
                .ignoreExceptions()
                .until(() -> kafka.topicExists(topicName));

        log.debug("Propagation done for topic: {}", topicName);
    }

    /**
     * Subscribes, seeks to beginning, then polls in a loop (main thread)
     * until at least {@code expectedCount} messages are collected or timeout.
     * <p>
     * Replaces: subscribe + seekToBeginning + Thread.sleep + pollMessages.
     *
     * @param kafka         facade
     * @param topicName     topic name
     * @param expectedCount minimum messages to collect
     * @param timeoutSec    max wait in seconds
     * @return collected messages (may be fewer than expectedCount on timeout)
     */
    public static List<Message> awaitMessages(KafkaTestFacade kafka,
                                              String topicName,
                                              int expectedCount,
                                              int timeoutSec) {
        log.debug("Awaiting {} messages from topic: {}", expectedCount, topicName);

        kafka.subscribe(topicName);
        kafka.seekToBeginning();

        return pollUntilCollected(kafka, expectedCount, timeoutSec);
    }

    /**
     * Polls in a loop (main thread) from the current consumer position
     * until at least {@code expectedCount} messages are collected or timeout.
     * <p>
     * Use after awaitConsumerReady() when consumer is already subscribed.
     * Replaces: Thread.sleep + pollMessages.
     *
     * @param kafka         facade
     * @param expectedCount minimum messages to collect
     * @param timeoutSec    max wait in seconds
     * @return collected messages
     */
    public static List<Message> awaitNewMessages(KafkaTestFacade kafka,
                                                 int expectedCount,
                                                 int timeoutSec) {
        log.debug("Awaiting {} new messages from current position", expectedCount);
        return pollUntilCollected(kafka, expectedCount, timeoutSec);
    }

    /**
     * After closing a consumer and creating a new one, waits for the new
     * consumer group rebalance to complete.
     * <p>
     * Rebalance is considered complete when <strong>all</strong> of the
     * following are true:
     * <ol>
     *   <li>A {@code poll()} call returns without {@code IllegalStateException}
     *       — meaning the consumer has joined the group and heartbeat is running.</li>
     *   <li>{@code getConsumerGroup().partitionAssignments()} is non-empty
     *       — confirming the coordinator has actually distributed partitions.</li>
     *   <li>{@code getConsumerGroup().getState() == STABLE}
     *       — the broker-side group FSM has left PREPARING_REBALANCE /
     *       COMPLETING_REBALANCE and settled.</li>
     * </ol>
     * <p>
     * All poll calls happen in the calling (main) thread via
     * {@code pollInSameThread()} so KafkaConsumer ThreadLocal is respected.
     *
     * @param kafka      facade (already subscribed, or subscribe will be called here)
     * @param topicName  topic to subscribe to
     * @param timeoutSec maximum seconds to wait for rebalance completion
     * @throws org.awaitility.core.ConditionTimeoutException if rebalance does
     *         not complete within {@code timeoutSec}
     */
    public static void awaitRebalance(KafkaTestFacade kafka,
                                      String topicName,
                                      int timeoutSec) {
        log.debug("Awaiting rebalance after consumer change on topic: {}", topicName);
        kafka.subscribe(topicName);

        // Phase 1 — wait until the local consumer has partitions assigned.
        //
        // poll() drives the JoinGroup / SyncGroup Kafka protocol: without it,
        // the broker never delivers the partition assignment to this client.
        // isAssigned() reads KafkaConsumer.assignment() — a local, in-memory
        // set, no network call — and returns true only when the rebalance
        // protocol has finished and at least one partition is owned.
        //
        // ALL calls run in the calling (main) thread via pollInSameThread()
        // because KafkaConsumer is ThreadLocal and must never be touched from
        // any other thread.
        await("local partition assignment: " + topicName)
                .atMost(timeoutSec, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(500))
                .pollInSameThread()
                .ignoreException(IllegalStateException.class)
                .until(() -> {
                    // Drives JoinGroup/SyncGroup protocol — mandatory for assignment delivery.
                    kafka.poll(Duration.ofMillis(300));
                    // Local read — true only after partitions are actually assigned.
                    boolean assigned = kafka.isAssigned();
                    log.trace("Rebalance phase-1: isAssigned={} on '{}'", assigned, topicName);
                    return assigned;
                });

        // Phase 2 — verify broker-side STABLE state via AdminClient.
        //
        // After local assignment is confirmed, check the broker's view exactly
        // once to rule out the rare race where the coordinator starts a second
        // rebalance immediately after the first (e.g. another consumer joining
        // the same group). AdminClient is thread-safe — safe from the main thread.
        ConsumerGroup group = kafka.getConsumerGroup();
        if (group.getState() != ConsumerGroup.GroupState.STABLE) {
            log.warn("Partitions assigned locally but broker reports state={} on '{}'. Waiting for STABLE...",
                    group.getState(), topicName);

            await("broker STABLE state: " + topicName)
                    .atMost(Math.max(timeoutSec / 2, 5), TimeUnit.SECONDS)
                    .pollInterval(Duration.ofMillis(500))
                    .pollInSameThread()
                    .until(() -> {
                        kafka.poll(Duration.ofMillis(300));
                        ConsumerGroup g = kafka.getConsumerGroup();
                        log.debug("Rebalance phase-2: brokerState={} on '{}'", g.getState(), topicName);
                        return g.getState() == ConsumerGroup.GroupState.STABLE;
                    });
        }

        log.info("Rebalance complete on topic '{}': partitions assigned, broker state=STABLE", topicName);
    }

    /**
     * Polls in a tight loop in the calling thread until expectedCount messages
     * are collected or the timeout expires.
     * <p>
     * This is the core loop — runs entirely in main thread, no Awaitility
     * threading involved for the poll calls themselves.
     */
    private static List<Message> pollUntilCollected(KafkaTestFacade kafka,
                                                    int expectedCount,
                                                    int timeoutSec) {
        List<Message> collected = new ArrayList<>();
        Instant deadline = Instant.now().plusSeconds(timeoutSec);

        while (Instant.now().isBefore(deadline)) {
            ConsumeResult result = kafka.poll(Duration.ofSeconds(2));

            if (result != null && result.isSuccess() && result.getMessageCount() > 0) {
                collected.addAll(result.getMessages());
                log.debug("Collected {}/{} messages", collected.size(), expectedCount);

                if (collected.size() >= expectedCount) {
                    break;
                }
            }

            // Brief Awaitility-based pause between polls (no Thread.sleep)
            awaitMillis(200);
        }

        if (collected.size() < expectedCount) {
            log.warn("Timeout: collected {}/{} messages from topic in {} sec",
                    collected.size(), expectedCount, timeoutSec);
        } else {
            log.debug("Successfully collected {} messages", collected.size());
        }

        return collected;
    }

    /**
     * Non-blocking pause via Awaitility (replaces Thread.sleep for inter-poll waits).
     */
    private static void awaitMillis(long millis) {
        await()
                .pollDelay(Duration.ofMillis(millis))
                .atMost(Duration.ofMillis(millis + 100))
                .until(() -> true);
    }
}
