package qa.autotest.framework.utils;

import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import qa.autotest.framework.application.service.KafkaTestFacade;
import qa.autotest.framework.domain.model.ConsumeResult;
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

    // ── Consumer readiness ────────────────────────────────────────────────────

    /**
     * Subscribes to topic, then waits until the consumer group is joined
     * and partitions are assigned.
     * <p>
     * All poll calls happen in the calling (main) thread via pollInSameThread().
     * Replaces: subscribe() + Thread.sleep(3000) + pre-warm poll.
     *
     * @param kafka      facade
     * @param topicName  topic to subscribe to
     * @param timeoutSec max wait in seconds
     */
    public static void awaitConsumerReady(KafkaTestFacade kafka,
                                          String topicName,
                                          int timeoutSec) {
        log.debug("Subscribing and awaiting consumer ready: {}", topicName);
        kafka.subscribe(topicName);

        // pollInSameThread() — condition runs in the calling thread,
        // so KafkaConsumer ThreadLocal is accessible.
        await("consumer group joined: " + topicName)
                .atMost(timeoutSec, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(500))
                .pollInSameThread()
                .ignoreException(IllegalStateException.class)
                .until(() -> {
                    // A successful short poll means partitions are assigned
                    ConsumeResult r = kafka.poll(Duration.ofMillis(300));
                    return r != null; // poll returned without IllegalStateException
                });

        log.debug("Consumer ready on topic: {}", topicName);
    }

    // ── Publish propagation ───────────────────────────────────────────────────

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

    // ── Message consumption ───────────────────────────────────────────────────

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

    // ── Rebalance ─────────────────────────────────────────────────────────────

    /**
     * After closing a consumer and creating a new one, waits for the new
     * consumer group rebalance to complete.
     * <p>
     * All poll calls happen in the calling (main) thread via pollInSameThread().
     * Replaces: Thread.sleep(5000) after close + subscribe.
     *
     * @param kafka      new facade (already has subscribe called on it OR will subscribe)
     * @param topicName  topic to subscribe
     * @param timeoutSec max wait in seconds
     */
    public static void awaitRebalance(KafkaTestFacade kafka,
                                      String topicName,
                                      int timeoutSec) {
        log.debug("Awaiting rebalance after consumer change on topic: {}", topicName);
        kafka.subscribe(topicName);

        await("rebalance complete: " + topicName)
                .atMost(timeoutSec, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(500))
                .pollInSameThread()
                .ignoreException(IllegalStateException.class)
                .until(() -> {
                    kafka.poll(Duration.ofMillis(300));
                    return true;
                });

        log.debug("Rebalance complete on topic: {}", topicName);
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

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
