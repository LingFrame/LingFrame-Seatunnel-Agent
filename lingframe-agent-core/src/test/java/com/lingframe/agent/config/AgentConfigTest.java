package com.lingframe.agent.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("AgentConfig 配置加载测试")
class AgentConfigTest {

    @Nested
    @DisplayName("默认配置降级")
    class DefaultConfig {

        @Test
        @DisplayName("无配置文件时应返回默认值（弹性治理默认关闭）")
        void shouldReturnDefaultsWhenNoConfig() {
            final AgentConfig config = AgentConfig.load(null);
            // Agent 激活、ClassLoader 清理/TCCL 防御默认开启，弹性治理默认关闭
            assertThat(config.isGovernanceEnabled()).isTrue();
            assertThat(config.isResilienceEnabled()).isFalse();
            assertThat(config.isCircuitBreakerEnabled()).isFalse();
            assertThat(config.isRateLimiterEnabled()).isFalse();
            assertThat(config.isBulkheadEnabled()).isFalse();
            assertThat(config.isTimeoutEnabled()).isFalse();
            assertThat(config.isGrayRoutingEnabled()).isFalse();
            assertThat(config.isPermissionEnabled()).isFalse();
            // 默认无弹性治理特性，不织入批次切点
            assertThat(config.isTaskExecutionAdviceEnabled()).isFalse();
            assertThat(config.isEffectiveTaskExecutionAdviceEnabled()).isFalse();
        }
    }

    @Nested
    @DisplayName("YAML 配置解析")
    class YamlParsing {

        @Test
        @DisplayName("应正确解析治理关闭配置")
        void shouldParseGovernanceDisabled(@TempDir Path tempDir) throws IOException {
            final Path configPath = tempDir.resolve("lingframe-governance.yaml");
            Files.write(configPath, (
                    "governance:\n" +
                    "  enabled: false\n"
            ).getBytes());

            final AgentConfig config = AgentConfig.load(configPath.toString());
            assertThat(config.isGovernanceEnabled()).isFalse();
        }

        @Test
        @DisplayName("应正确解析完整治理配置")
        void shouldParseFullConfig(@TempDir Path tempDir) throws IOException {
            final Path configPath = tempDir.resolve("lingframe-governance.yaml");
            Files.write(configPath, (
                    "governance:\n" +
                    "  enabled: true\n" +
                    "  resilience:\n" +
                    "    enabled: false\n" +
                    "    circuit-breaker-enabled: false\n" +
                    "    rate-limiter-enabled: true\n" +
                    "    bulkhead-enabled: false\n" +
                    "    timeout-enabled: false\n" +
                    "  routing:\n" +
                    "    gray-routing-enabled: true\n" +
                    "  security:\n" +
                    "    permission-enabled: true\n"
            ).getBytes());

            final AgentConfig config = AgentConfig.load(configPath.toString());
            assertThat(config.isGovernanceEnabled()).isTrue();
            assertThat(config.isResilienceEnabled()).isFalse();
            assertThat(config.isCircuitBreakerEnabled()).isFalse();
            assertThat(config.isRateLimiterEnabled()).isTrue();
            assertThat(config.isBulkheadEnabled()).isFalse();
            assertThat(config.isTimeoutEnabled()).isFalse();
            assertThat(config.isGrayRoutingEnabled()).isTrue();
            assertThat(config.isPermissionEnabled()).isTrue();
        }
    }
}
