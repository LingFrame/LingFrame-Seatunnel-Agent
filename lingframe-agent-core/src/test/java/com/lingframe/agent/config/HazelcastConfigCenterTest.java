package com.lingframe.agent.config;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.lingframe.core.event.EventBus;
import com.lingframe.core.fsm.RuntimeCoordinator;
import com.lingframe.core.ling.DefaultLingRepository;
import com.lingframe.core.ling.LingRepository;
import com.lingframe.core.ling.LingRuntime;
import com.lingframe.core.ling.LingRuntimeConfig;
import com.lingframe.core.ling.VirtualLingManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayName("HazelcastConfigCenter 分布式配置中心测试")
class HazelcastConfigCenterTest {

    @Nested
    @DisplayName("无 Hazelcast 实例时的降级行为")
    class NoHazelcastInstance {

        @Test
        @DisplayName("tryInit 在无 Hazelcast 实例时应返回 false")
        void shouldReturnFalseWhenNoInstance() {
            final HazelcastConfigCenter center = new HazelcastConfigCenter(null);
            final boolean result = center.tryInit();
            assertThat(result).isFalse();
            assertThat(center.isInitialized()).isFalse();
        }

        @Test
        @DisplayName("stop 应安全执行不抛异常")
        void shouldStopSafely() {
            final HazelcastConfigCenter center = new HazelcastConfigCenter(null);
            center.stop();
            assertThat(center.isInitialized()).isFalse();
        }
    }

    @Nested
    @DisplayName("动态配置监听与虚拟灵元热刷新")
    class DynamicConfigUpdate {

        private LingRepository lingRepository;
        private VirtualLingManager virtualLingManager;
        private HazelcastInstance mockInstance;
        private IMap<String, String> mockMap;
        private Map<String, String> backingData;
        private HazelcastConfigCenter configCenter;
        private EntryAddedListener<String, String> addedListener;
        private EntryUpdatedListener<String, String> updatedListener;
        private EntryRemovedListener<String, String> removedListener;
        private UUID listenerId;

        @BeforeEach
        @SuppressWarnings("unchecked")
        void setUp() {
            lingRepository = new DefaultLingRepository();
            final EventBus eventBus = new EventBus();
            final RuntimeCoordinator runtimeCoordinator = new RuntimeCoordinator(eventBus);
            virtualLingManager = new VirtualLingManager(lingRepository, runtimeCoordinator, eventBus);

            // 注册默认虚拟灵元 seatunnel
            virtualLingManager.register("seatunnel", LingRuntimeConfig.defaults());

            mockMap = mock(IMap.class);
            backingData = new HashMap<>();

            when(mockMap.get(any())).thenAnswer(invocation -> backingData.get(invocation.getArgument(0)));

            listenerId = UUID.randomUUID();
            final ArgumentCaptor<MapListener> listenerCaptor = ArgumentCaptor.forClass(MapListener.class);
            when(mockMap.addEntryListener(listenerCaptor.capture(), eq(true))).thenReturn(listenerId);

            mockInstance = mock(HazelcastInstance.class);
            when(mockInstance.getName()).thenReturn("test-hazelcast-instance");
            doReturn(mockMap).when(mockInstance).getMap(HazelcastConfigCenter.CONFIG_IMAP_NAME);

            configCenter = new HazelcastConfigCenter(lingRepository, mockInstance);
            final boolean initialized = configCenter.tryInit();
            assertThat(initialized).isTrue();

            // 捕获注册的监听器
            final List<MapListener> capturedListeners = listenerCaptor.getAllValues();
            for (MapListener listener : capturedListeners) {
                if (listener instanceof EntryAddedListener) {
                    addedListener = (EntryAddedListener<String, String>) listener;
                }
                if (listener instanceof EntryUpdatedListener) {
                    updatedListener = (EntryUpdatedListener<String, String>) listener;
                }
                if (listener instanceof EntryRemovedListener) {
                    removedListener = (EntryRemovedListener<String, String>) listener;
                }
            }
        }

        @Test
        @DisplayName("初始化时应从 IMap 读取初始配置并应用到虚拟灵元")
        void shouldApplyInitialConfigFromMap() {
            assertThat(configCenter.isInitialized()).isTrue();
            final LingRuntime runtime = lingRepository.getRuntime("seatunnel");
            assertThat(runtime).isNotNull();
            assertThat(runtime.getConfig().getRateLimitPerSecond()).isEqualTo(0);
        }

        @Test
        @DisplayName("EntryUpdated 事件触发时应毫秒级热更新虚拟灵元限流与熔断参数")
        void shouldUpdateVirtualLingConfigOnEntryUpdated() {
            backingData.put(HazelcastConfigCenter.KEY_RATE_LIMIT, "600");
            backingData.put(HazelcastConfigCenter.KEY_CB_FAILURE_RATE, "35");
            backingData.put(HazelcastConfigCenter.KEY_CB_SLIDING_WINDOW, "50");
            backingData.put(HazelcastConfigCenter.KEY_DEFAULT_TIMEOUT, "5000");

            final EntryEvent<String, String> event = new EntryEvent<>(
                    "test-source", null, EntryEventType.UPDATED.getType(),
                    HazelcastConfigCenter.KEY_RATE_LIMIT, "600"
            );

            assertThat(updatedListener).isNotNull();
            updatedListener.entryUpdated(event);

            final LingRuntime runtime = lingRepository.getRuntime("seatunnel");
            assertThat(runtime).isNotNull();
            assertThat(runtime.getConfig().getRateLimitPerSecond()).isEqualTo(600);
            assertThat(runtime.getConfig().getCircuitBreakerFailureRateThreshold()).isEqualTo(35);
            assertThat(runtime.getConfig().getCircuitBreakerSlidingWindowSize()).isEqualTo(50);
            assertThat(runtime.getConfig().getDefaultTimeoutMs()).isEqualTo(5000);
        }

        @Test
        @DisplayName("EntryAdded 事件触发时应正确刷新虚拟灵元配置")
        void shouldUpdateVirtualLingConfigOnEntryAdded() {
            backingData.put(HazelcastConfigCenter.KEY_RATE_LIMIT, "1200");

            final EntryEvent<String, String> event = new EntryEvent<>(
                    "test-source", null, EntryEventType.ADDED.getType(),
                    HazelcastConfigCenter.KEY_RATE_LIMIT, "1200"
            );

            assertThat(addedListener).isNotNull();
            addedListener.entryAdded(event);

            final LingRuntime runtime = lingRepository.getRuntime("seatunnel");
            assertThat(runtime).isNotNull();
            assertThat(runtime.getConfig().getRateLimitPerSecond()).isEqualTo(1200);
        }

        @Test
        @DisplayName("EntryRemoved 事件触发后缺失 Key 应安全回退默认值")
        void shouldFallbackGracefullyOnEntryRemoved() {
            // 先设置一个高限流
            backingData.put(HazelcastConfigCenter.KEY_RATE_LIMIT, "2000");
            final EntryEvent<String, String> addEvent = new EntryEvent<>(
                    "test-source", null, EntryEventType.ADDED.getType(),
                    HazelcastConfigCenter.KEY_RATE_LIMIT, "2000"
            );
            addedListener.entryAdded(addEvent);
            assertThat(lingRepository.getRuntime("seatunnel").getConfig().getRateLimitPerSecond()).isEqualTo(2000);

            // 模拟配置项被删除
            backingData.remove(HazelcastConfigCenter.KEY_RATE_LIMIT);
            final EntryEvent<String, String> removeEvent = new EntryEvent<>(
                    "test-source", null, EntryEventType.REMOVED.getType(),
                    HazelcastConfigCenter.KEY_RATE_LIMIT, null
            );
            assertThat(removedListener).isNotNull();
            removedListener.entryRemoved(removeEvent);

            // 读取缺失的 key 会回退到现有 config 的值，保证业务不抖动
            assertThat(lingRepository.getRuntime("seatunnel").getConfig().getRateLimitPerSecond()).isEqualTo(2000);
        }

        @Test
        @DisplayName("非法非数字配置值应容错回退而不抛出异常")
        void shouldHandleInvalidNumberGracefully() {
            backingData.put(HazelcastConfigCenter.KEY_RATE_LIMIT, "invalid-qps");
            final EntryEvent<String, String> event = new EntryEvent<>(
                    "test-source", null, EntryEventType.UPDATED.getType(),
                    HazelcastConfigCenter.KEY_RATE_LIMIT, "invalid-qps"
            );

            updatedListener.entryUpdated(event);

            // 保持原有值，不崩溃
            assertThat(lingRepository.getRuntime("seatunnel").getConfig().getRateLimitPerSecond()).isEqualTo(0);
        }

        @Test
        @DisplayName("调用 stop 应注销所有已注册的 EntryListener 并重置 initialized 状态")
        void shouldRemoveListenersOnStop() {
            assertThat(configCenter.isInitialized()).isTrue();

            configCenter.stop();

            assertThat(configCenter.isInitialized()).isFalse();
            // 5 类事件共 5 个监听器
            verify(mockMap, times(5)).removeEntryListener(eq(listenerId));
        }
    }
}
