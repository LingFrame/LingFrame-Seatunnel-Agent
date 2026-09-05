package com.lingframe.agent.pipeline;

import com.lingframe.agent.config.AgentConfig;
import com.lingframe.agent.config.TestAgentConfigs;
import com.lingframe.core.fsm.RuntimeStatus;
import com.lingframe.core.ling.LingRuntime;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("AgentPipelineFactory 治理流水线工厂测试")
class AgentPipelineFactoryTest {

    @Nested
    @DisplayName("正常装配")
    class NormalAssembly {

        @Test
        @DisplayName("create 应返回非 null 的治理运行时")
        void shouldReturnNonNullPipelineEngine() {
            final AgentConfig config = TestAgentConfigs.create(true, true, true, false, false, true);
            final AgentGovernanceRuntime runtime = AgentPipelineFactory.create(config);
            assertThat(runtime).isNotNull();
            assertThat(runtime.getPipelineEngine()).isNotNull();
            assertThat(runtime.getUnloadCoordinator()).isNotNull();
            assertThat(runtime.getLingRepository()).isNotNull();
        }

        @Test
        @DisplayName("多次 create 应返回独立实例")
        void shouldReturnIndependentInstances() {
            final AgentConfig config = TestAgentConfigs.create(true, true, true, false, false, true);
            final AgentGovernanceRuntime runtime1 = AgentPipelineFactory.create(config);
            final AgentGovernanceRuntime runtime2 = AgentPipelineFactory.create(config);
            assertThat(runtime1).isNotSameAs(runtime2);
        }
    }

    @Nested
    @DisplayName("虚拟灵元注册")
    class VirtualLingRegistration {

        @Test
        @DisplayName("resilience 启用时应注册虚拟灵元 seatunnel 为 ACTIVE")
        void shouldRegisterVirtualLingAsActive() {
            final AgentConfig config = TestAgentConfigs.create(true, true, true, false, false, true);
            final AgentGovernanceRuntime runtime = AgentPipelineFactory.create(config);

            final LingRuntime virtualLing = runtime.getLingRepository().getRuntime("seatunnel");
            assertThat(virtualLing).isNotNull();
            assertThat(virtualLing.currentStatus()).isEqualTo(RuntimeStatus.ACTIVE);
            assertThat(virtualLing.isVirtual()).isTrue();
            assertThat(virtualLing.isAvailable()).isTrue();
            assertThat(virtualLing.getInstancePool()).isNull();
            assertThat(virtualLing.getReadyInstances()).isEmpty();
        }

        @Test
        @DisplayName("resilience 关闭时不应注册虚拟灵元")
        void shouldNotRegisterVirtualLingWhenResilienceDisabled() {
            final AgentConfig config = TestAgentConfigs.create(true, false, false, false, false, true);
            final AgentGovernanceRuntime runtime = AgentPipelineFactory.create(config);

            final LingRuntime virtualLing = runtime.getLingRepository().getRuntime("seatunnel");
            assertThat(virtualLing).isNull();
        }

        @Test
        @DisplayName("虚拟灵元 config 应包含限流和熔断参数")
        void shouldContainResilienceParams() {
            final AgentConfig config = TestAgentConfigs.create(true, true, true, false, false, true);
            final AgentGovernanceRuntime runtime = AgentPipelineFactory.create(config);

            final LingRuntime virtualLing = runtime.getLingRepository().getRuntime("seatunnel");
            assertThat(virtualLing).isNotNull();
            assertThat(virtualLing.getConfig().getRateLimitPerSecond()).isEqualTo(100);
            assertThat(virtualLing.getConfig().getCircuitBreakerFailureRateThreshold()).isEqualTo(50);
            assertThat(virtualLing.getConfig().getCircuitBreakerSlidingWindowSize()).isEqualTo(20);
        }
    }

    @Nested
    @DisplayName("devMode 配置传递")
    class DevModePropagation {

        @Test
        @DisplayName("devMode=false 时应创建 prod 模式 pipeline")
        void shouldCreateProdPipeline() {
            final AgentConfig config = TestAgentConfigs.create(true, true, true, false, false, false);
            final AgentGovernanceRuntime runtime = AgentPipelineFactory.create(config);
            assertThat(runtime.getPipelineEngine()).isNotNull();
        }
    }
}
