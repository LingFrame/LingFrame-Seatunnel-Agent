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
        @DisplayName("无配置文件时应返回默认值（默认不开启批次级治理：弹性特性与切点全关）")
        void shouldReturnDefaultsWhenNoConfig() {
            final AgentConfig config = AgentConfig.load(null);
            // 总开关开启（Agent 激活、ClassLoader 清理/TCCL 防御生效），但默认不装配任何治理特性
            assertThat(config.isGovernanceEnabled()).isTrue();
            assertThat(config.isCircuitBreakerEnabled()).isFalse();
            assertThat(config.isRateLimiterEnabled()).isFalse();
            assertThat(config.isGrayRoutingEnabled()).isFalse();
            assertThat(config.isPermissionEnabled()).isFalse();
            // 默认无治理特性 → 批次切点有效开关为 false（不织入 call()，纯 ClassLoader 清理）
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
                    "    circuit-breaker-enabled: false\n" +
                    "    rate-limiter-enabled: true\n" +
                    "  routing:\n" +
                    "    gray-routing-enabled: true\n" +
                    "  security:\n" +
                    "    permission-enabled: true\n"
            ).getBytes());

            final AgentConfig config = AgentConfig.load(configPath.toString());
            assertThat(config.isGovernanceEnabled()).isTrue();
            assertThat(config.isCircuitBreakerEnabled()).isFalse();
            assertThat(config.isRateLimiterEnabled()).isTrue();
            assertThat(config.isGrayRoutingEnabled()).isTrue();
            assertThat(config.isPermissionEnabled()).isTrue();
        }
    }
}
