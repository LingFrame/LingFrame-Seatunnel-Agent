package com.lingframe.agent.advice;

import com.lingframe.agent.adapter.SeaTunnelAdapter;
import com.lingframe.agent.bridge.LingFrameAgentBridge;
import com.lingframe.agent.config.AgentConfig;
import com.lingframe.agent.config.TestAgentConfigs;
import com.lingframe.core.pipeline.InvocationPipelineEngine;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("ClassLoaderReleaseAdvice 双态自适应清理测试")
class ClassLoaderReleaseAdviceTest {

    private ClassLoaderReleaseAdviceTest() {
    }

    @BeforeAll
    static void setupBridge() {
        final AgentConfig config = TestAgentConfigs.create(true, true, true, false, false, false);
        final InvocationPipelineEngine pipeline = new InvocationPipelineEngine(null);
        LingFrameAgentBridge.registerContract(new SeaTunnelAdapter(config, pipeline, null, null, null, null, null));
    }

    @AfterAll
    static void teardownBridge() {
        LingFrameAgentBridge.registerContract(null);
    }

    @Nested
    @DisplayName("onEnter ClassLoader 提取")
    class OnEnterExtraction {

        @Test
        @DisplayName("cacheMode=true 时 jobId 应重定向为 1L")
        void shouldRedirectJobIdTo1LWhenCacheMode() {
            final ClassLoader targetLoader = new ClassLoader() { };
            final Collection<URL> jars = Collections.emptyList();
            final String key = LingFrameAgentBridge.convertJarsToKey(jars);
            final Map<String, ClassLoader> jobMap = new HashMap<>();
            jobMap.put(key, targetLoader);
            final Map<Long, Map<String, ClassLoader>> cache = new HashMap<>();
            cache.put(1L, jobMap);

            final ClassLoader result = ClassLoaderReleaseAdvice.onEnter(
                    true, cache, 999L, jars);

            assertThat(result).isSameAs(targetLoader);
        }

        @Test
        @DisplayName("cacheMode=false 时 jobId 应保持原值")
        void shouldKeepOriginalJobIdWhenNoCache() {
            final ClassLoader targetLoader = new ClassLoader() { };
            final Collection<URL> jars = Collections.emptyList();
            final String key = LingFrameAgentBridge.convertJarsToKey(jars);
            final Map<String, ClassLoader> jobMap = new HashMap<>();
            jobMap.put(key, targetLoader);
            final Map<Long, Map<String, ClassLoader>> cache = new HashMap<>();
            cache.put(42L, jobMap);

            final ClassLoader result = ClassLoaderReleaseAdvice.onEnter(
                    false, cache, 42L, jars);

            assertThat(result).isSameAs(targetLoader);
        }

        @Test
        @DisplayName("jobMap 为 null 时应返回 null")
        void shouldReturnNullWhenJobMapMissing() {
            final Map<Long, Map<String, ClassLoader>> cache = new HashMap<>();

            final ClassLoader result = ClassLoaderReleaseAdvice.onEnter(
                    false, cache, 99L, Collections.emptyList());

            assertThat(result).isNull();
        }
    }

    @Nested
    @DisplayName("onExit 清理与释放")
    class OnExitCleanup {

        @Test
        @DisplayName("targetLoader 为 null 时应安全跳过")
        void shouldSkipWhenTargetLoaderNull() {
            ClassLoaderReleaseAdvice.onExit(
                    false, new HashMap<>(), 1L, Collections.emptyList(), null);
        }

        @Test
        @DisplayName("cacheMode=true 时不应触发物理释放")
        void shouldNotTriggerPhysicalReleaseWhenCacheMode() {
            final ClassLoader targetLoader = new ClassLoader() { };
            final Map<String, ClassLoader> jobMap = new HashMap<>();
            jobMap.put("key", targetLoader);
            final Map<Long, Map<String, ClassLoader>> cache = new HashMap<>();
            cache.put(1L, jobMap);

            ClassLoaderReleaseAdvice.onExit(
                    true, cache, 1L, Collections.emptyList(), targetLoader);
        }

        @Test
        @DisplayName("cacheMode=false 且 Key 已移除时应触发物理释放")
        void shouldTriggerPhysicalReleaseWhenKeyRemoved() {
            final ClassLoader targetLoader = new URLClassLoader(new URL[0], ClassLoader.getSystemClassLoader());
            final Map<Long, Map<String, ClassLoader>> cache = new HashMap<>();

            ClassLoaderReleaseAdvice.onExit(
                    false, cache, 1L, Collections.emptyList(), targetLoader);
        }

        @Test
        @DisplayName("cacheMode=false 且 Key 仍存在时不应触发物理释放")
        void shouldNotTriggerPhysicalReleaseWhenKeyExists() {
            final ClassLoader targetLoader = new ClassLoader() { };
            final Map<String, ClassLoader> jobMap = new HashMap<>();
            final Collection<URL> emptyJars = Collections.emptyList();
            jobMap.put(LingFrameAgentBridge.convertJarsToKey(emptyJars), targetLoader);
            final Map<Long, Map<String, ClassLoader>> cache = new HashMap<>();
            cache.put(1L, jobMap);

            ClassLoaderReleaseAdvice.onExit(
                    false, cache, 1L, emptyJars, targetLoader);
        }
    }
}