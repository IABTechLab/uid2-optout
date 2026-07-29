package com.uid2.optout;

import com.uid2.shared.health.HealthManager;
import com.uid2.shared.health.IHealthComponent;
import com.uid2.shared.health.PodTerminationMonitor;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class MainTest {
    @TempDir
    Path tempDir;

    private HealthManager originalHealthManager;
    private CapturingHealthManager healthManager;

    @BeforeEach
    void setUp() {
        originalHealthManager = HealthManager.instance;
        healthManager = new CapturingHealthManager();
        HealthManager.instance = healthManager;
    }

    @AfterEach
    void tearDown() {
        HealthManager.instance = originalHealthManager;
    }

    @Test
    void constructorRegistersPodTerminationMonitorWithDefaultInterval() throws Exception {
        new Main(null, createConfig());

        PodTerminationMonitor monitor = assertInstanceOf(
                PodTerminationMonitor.class, healthManager.registeredComponent);
        assertEquals(3000L, getFileCheckInterval(monitor));
    }

    @Test
    void constructorUsesConfiguredPodTerminationCheckInterval() throws Exception {
        JsonObject config = createConfig()
                .put(Const.Config.PodTerminationCheckInterval, 1250);

        new Main(null, config);

        PodTerminationMonitor monitor = assertInstanceOf(
                PodTerminationMonitor.class, healthManager.registeredComponent);
        assertEquals(1250L, getFileCheckInterval(monitor));
    }

    private JsonObject createConfig() throws Exception {
        JsonObject config = new JsonObject(Files.readString(Path.of("conf/default-config.json")));
        config.mergeIn(new JsonObject(Files.readString(Path.of("conf/local-config.json"))));
        return config.put(Const.Config.OptOutDataDirProp, tempDir.toString());
    }

    private static long getFileCheckInterval(PodTerminationMonitor monitor) throws Exception {
        var field = PodTerminationMonitor.class.getDeclaredField("fileCheckIntervalMs");
        field.setAccessible(true);
        return field.getLong(monitor);
    }

    private static class CapturingHealthManager extends HealthManager {
        private IHealthComponent registeredComponent;

        @Override
        public synchronized <T extends IHealthComponent> T registerGenericComponent(T component) {
            registeredComponent = component;
            return component;
        }
    }
}
