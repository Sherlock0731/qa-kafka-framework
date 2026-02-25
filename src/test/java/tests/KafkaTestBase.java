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
 * <p>
 * Единственная задача: инициализировать {@link KafkaTestFacade} перед каждым тестом,
 * закрыть ресурсы после него и гарантировать глобальную очистку всех адаптеров
 * из всех потоков по завершении класса.
 *
 * <h3>Иерархия наследования</h3>
 * <pre>
 *   KafkaTestBase          ← lifecycle (@BeforeAll/Each, @AfterEach/All)
 *       ↑
 *   KafkaTestHelpers       ← domain helpers (createTopic, publish, consume)
 *       ↑
 *   BaseTest               ← точка входа для всех тест-классов
 * </pre>
 *
 * <h3>WeakHashMap + ALL_FACADES</h3>
 * Используется WeakHashMap-backed set: если локальная переменная {@code kafka}
 * обнулилась после теста, GC может собрать facade до {@code @AfterAll}, не допуская
 * накопления мёртвых ссылок при большом числе тестов.
 * {@code @AfterAll} — страховочный слой: явно закрывает все живые facades.
 */
@Slf4j
@ExtendWith({
        TestMetricsExtension.class,
        AllureKafkaListener.class,
        KafkaTestExecutionListener.class
})
public abstract class KafkaTestBase {

    /**
     * Единственная точка доступа к конфигурации.
     */
    public static final KafkaConfig CONFIG = ConfigFactory.getConfig();

    /**
     * WeakHashMap-backed set для отслеживания всех живых facades.
     * Потокобезопасен через {@code Collections.synchronizedSet}.
     */
    private static final Set<KafkaTestFacade> ALL_FACADES =
            Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));

    /**
     * Facade текущего теста. Инициализируется в {@code @BeforeEach},
     * закрывается в {@code @AfterEach}.
     */
    public KafkaTestFacade kafka;

    /**
     * Имена топиков, созданных в рамках текущего теста.
     * Очищаются в {@code @AfterEach}.
     */
    public final List<String> createdTopics = new ArrayList<>();

    /**
     * Время начала теста (мс). Используется в {@code @AfterEach} для логирования длительности.
     */
    public long testStartTime;

    // ── @BeforeAll ────────────────────────────────────────────────────────────

    @BeforeAll
    static void initFramework() {
        log.info("=".repeat(80));
        log.info("Kafka Test Framework — Hexagonal Architecture");
        log.info("=".repeat(80));
        log.info("Bootstrap : {}", CONFIG.kafkaBootstrapServers());
        log.info("Protocol  : {}", CONFIG.securityProtocol());
        log.info("=".repeat(80));
    }

    // ── @BeforeEach ───────────────────────────────────────────────────────────

    @BeforeEach
    void setUp(TestInfo testInfo) {
        testStartTime = System.currentTimeMillis();
        log.info("=".repeat(80));
        log.info("START  {}", testInfo.getDisplayName());
        log.info("Thread {}", Thread.currentThread().getName());
        log.info("=".repeat(80));

        kafka = new KafkaTestFacade(CONFIG);
        ALL_FACADES.add(kafka);

        log.debug("Facade initialized");
    }

    // ── @AfterEach ────────────────────────────────────────────────────────────

    @AfterEach
    void tearDown(TestInfo testInfo) {
        long duration = System.currentTimeMillis() - testStartTime;
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

    // ── @AfterAll ─────────────────────────────────────────────────────────────

    /**
     * Глобальная очистка: закрывает все адаптеры из всех потоков.
     * <p>
     * Вызывает {@code closeAll()} на каждом живом facade — адаптеры используют
     * WeakReference-трекинг и закрывают клиенты из всех thread'ов ForkJoinPool.
     */
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

    // ── package-private helpers ───────────────────────────────────────────────

    /**
     * Регистрирует новый facade в {@code ALL_FACADES} для гарантированной очистки.
     * Вызывается из {@link KafkaTestHelpers#createNewFacade()}.
     */
    public KafkaTestFacade registerFacade(KafkaTestFacade facade) {
        ALL_FACADES.add(facade);
        return facade;
    }

    // ── private ───────────────────────────────────────────────────────────────

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
