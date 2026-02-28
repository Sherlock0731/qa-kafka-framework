package tests;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import qa.autotest.framework.application.service.KafkaTestFacade;
import qa.autotest.framework.config.ConfigFactory;
import qa.autotest.framework.config.KafkaConfig;
import qa.autotest.framework.metrics.TestMetricsExtension;
import tests.listeners.AllureKafkaListener;
import tests.listeners.KafkaTestExecutionListener;

import java.util.*;

/**
 * KafkaTestBase — Ответственность: жизненный цикл теста.
 *
 * <h3>MDC</h3>
 * MDC заполняется в {@link KafkaTestExecutionListener#beforeEach} и очищается
 * в {@link KafkaTestExecutionListener#afterEach}.  Все последующие вызовы
 * {@code log.*} в этом классе автоматически несут поля
 * {@code test.id}, {@code test.class}, {@code test.method}, {@code test.thread}
 * в каждой строке лога — явно выводить их через {@code log.info("Thread {}", ...)}
 * больше не нужно.
 *
 * <h3>Параллельный запуск</h3>
 * SLF4J MDC хранит значения в {@link ThreadLocal}: каждый поток видит только
 * свой контекст.  Атрибуция лога к тесту работает без дополнительной синхронизации.
 *
 * <h3>Порядок Extensions в @ExtendWith</h3>
 * {@code KafkaTestExecutionListener} стоит первым — его {@code beforeEach}
 * вызывается раньше {@code setUp()}, поэтому MDC уже заполнен к моменту
 * первого {@code log.*} в этом классе.  {@code afterEach} вызывается после
 * {@code tearDown()}, поэтому MDC ещё присутствует во время cleanup.
 *
 * <h3>Иерархия наследования</h3>
 * <pre>
 *   KafkaTestBase      ← lifecycle (@BeforeAll/Each, @AfterEach/All) + MDC
 *       ↑
 *   KafkaTestHelpers   ← domain helpers (createTopic, publish, consume)
 *       ↑
 *   BaseTest           ← точка входа для всех тест-классов
 * </pre>
 */
@Slf4j
@ExtendWith({
        KafkaTestExecutionListener.class,  // ← FIRST: populateMdc() до любых log.*
        TestMetricsExtension.class,
        AllureKafkaListener.class
})
public abstract class KafkaTestBase {

    /** Единственная точка доступа к конфигурации. */
    public static final KafkaConfig CONFIG = ConfigFactory.getConfig();

    /**
     * WeakHashMap-backed set для отслеживания всех живых facades.
     * Потокобезопасен через {@code Collections.synchronizedSet}.
     */
    private static final Set<KafkaTestFacade> ALL_FACADES =
            Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));

    /** Facade текущего теста. Инициализируется в {@code @BeforeEach}. */
    public KafkaTestFacade kafka;

    /** Имена топиков, созданных в рамках текущего теста. Очищаются в {@code @AfterEach}. */
    public final List<String> createdTopics = new ArrayList<>();

    /** Время начала теста (мс). */
    public long testStartTime;

    @BeforeAll
    static void initFramework() {
        log.info("=".repeat(80));
        log.info("Kafka Test Framework — Hexagonal Architecture");
        log.info("=".repeat(80));
        log.info("Bootstrap : {}", CONFIG.kafkaBootstrapServers());
        log.info("Protocol  : {}", CONFIG.securityProtocol());
        log.info("=".repeat(80));
    }

    /**
     * MDC уже заполнен к этому моменту — {@link KafkaTestExecutionListener}
     * зарегистрирован первым в {@code @ExtendWith} и его {@code beforeEach}
     * вызывается до этого метода.
     */
    @BeforeEach
    void setUp(TestInfo testInfo) {
        testStartTime = System.currentTimeMillis();
        log.info("=".repeat(80));
        log.info("START  {}", testInfo.getDisplayName());
        log.info("=".repeat(80));

        kafka = new KafkaTestFacade(CONFIG);
        ALL_FACADES.add(kafka);
        log.debug("Facade initialized");
    }

    @AfterEach
    void tearDown(TestInfo testInfo) {
        long duration = System.currentTimeMillis() - testStartTime;
        // MDC ещё присутствует — KafkaTestExecutionListener.afterEach вызывается после
        log.info("=".repeat(80));
        log.info("END    {} ({}ms)", testInfo.getDisplayName(), duration);
        log.info("=".repeat(80));

        try {
            cleanupTopics();
            kafka.close();
            log.debug("Teardown complete");
        } catch (Exception e) {
            log.error("Error during teardown", e);
        }
    }

    @AfterAll
    static void globalCleanup() {
        log.info("=".repeat(80));
        log.info("GLOBAL CLEANUP — closing ALL resources from ALL threads");

        int closed = 0, failed = 0;

        synchronized (ALL_FACADES) {
            for (KafkaTestFacade facade : ALL_FACADES) {
                try {
                    facade.closeAll();
                    closed++;
                } catch (Exception e) {
                    failed++;
                    log.error("Failed to close facade", e);
                }
            }
            ALL_FACADES.clear();
        }

        log.info("Closed: {}  Failed: {}", closed, failed);
        log.info("=".repeat(80));
    }

    public KafkaTestFacade registerFacade(KafkaTestFacade facade) {
        ALL_FACADES.add(facade);
        return facade;
    }

    private void cleanupTopics() {
        if (createdTopics.isEmpty()) return;
        log.debug("Cleaning up {} topics", createdTopics.size());
        for (String name : createdTopics) {
            try {
                kafka.deleteTopic(name);
            } catch (Exception e) {
                log.warn("Failed to delete topic: {}", name);
            }
        }
        createdTopics.clear();
    }
}
