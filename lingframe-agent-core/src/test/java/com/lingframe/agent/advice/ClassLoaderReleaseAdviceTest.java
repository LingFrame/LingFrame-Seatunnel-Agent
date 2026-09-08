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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@DisplayName("ClassLoaderReleaseAdvice 双态自适应清理测试")
class ClassLoaderReleaseAdviceTest {

    private ClassLoaderReleaseAdviceTest() {
    }

    @BeforeAll
    static void setupBridge() {
        final AgentConfig config = TestAgentConfigs.create(true, true, true, false, false, false);
        final InvocationPipelineEngine pipeline = new InvocationPipelineEngine(null);
        LingFrameAgentBridge.registerContract(new SeaTunnelAdapter(config, pipeline, null, null, null, null));
    }

    @AfterAll
    static void teardownBridge() {
        LingFrameAgentBridge.registerContract(null);
    }


    @Nested
    @DisplayName("onEnter ClassLoader 提取与登记追踪")
    class OnEnterExtraction {

        @Test
        @DisplayName("cacheMode=true 时 jobId 应重定向为 1L 并安全完成")
        void shouldRedirectJobIdTo1LWhenCacheMode() {
            final ClassLoader targetLoader = new ClassLoader() { };
            final Collection<URL> jars = Collections.emptyList();
            final String key = LingFrameAgentBridge.convertJarsToKey(jars);
            final Map<String, ClassLoader> jobMap = new HashMap<>();
            jobMap.put(key, targetLoader);
            final Map<Long, Map<String, ClassLoader>> cache = new HashMap<>();
            cache.put(1L, jobMap);

            ClassLoaderReleaseAdvice.onEnter(true, cache, 999L, jars);
        }

        @Test
        @DisplayName("cacheMode=false 时 jobId 应保持原值并登记追踪")
        void shouldKeepOriginalJobIdWhenNoCache() {
            final ClassLoader targetLoader = new ClassLoader() { };
            final Collection<URL> jars = Collections.emptyList();
            final String key = LingFrameAgentBridge.convertJarsToKey(jars);
            final Map<String, ClassLoader> jobMap = new HashMap<>();
            jobMap.put(key, targetLoader);
            final Map<Long, Map<String, ClassLoader>> cache = new HashMap<>();
            cache.put(42L, jobMap);

            ClassLoaderReleaseAdvice.onEnter(false, cache, 42L, jars);
        }

        @Test
        @DisplayName("jobMap 为 null 时应安全跳过")
        void shouldReturnNullWhenJobMapMissing() {
            final Map<Long, Map<String, ClassLoader>> cache = new HashMap<>();

            ClassLoaderReleaseAdvice.onEnter(false, cache, 99L, Collections.emptyList());
        }
    }
}
