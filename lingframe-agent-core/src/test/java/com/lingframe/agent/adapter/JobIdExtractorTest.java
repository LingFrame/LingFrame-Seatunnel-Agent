package com.lingframe.agent.adapter;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * JobIdExtractor 单元测试。
 * <p>
 * 覆盖诚实降级语义：作业级能力位关闭或字段缺失时提取返回 {@link JobIdExtractor#NO_JOB}，
 * 由调用方回退共享灵元，绝不猜测字段名。
 */
@DisplayName("JobIdExtractor 作业身份解析测试")
class JobIdExtractorTest {

    /** 兼容 2.3.8 / 3.0.0-SNAPSHOT 的 AbstractTask 形态：`protected final long jobID`。 */
    static class FakeTask {
        protected final long jobID;

        FakeTask(long jobID) {
            this.jobID = jobID;
        }
    }

    /** 未携带 jobID 字段的任务类型（对应 SeaTunnel 演进改名后的未知版本）。 */
    static class NoJobTask {
    }

    /** 沿继承链上溯：jobID 声明在父类，子类应同样可解析。 */
    static class SubTask extends FakeTask {
        SubTask(long jobID) {
            super(jobID);
        }
    }

    @Nested
    @DisplayName("能力位关闭时")
    class CapabilityDisabled {

        @Test
        @DisplayName("isJobLevelSupported 应为 false，extract 恒返回 NO_JOB")
        void shouldReturnNoJobWhenCapabilityDisabled() {
            final JobIdExtractor extractor = new JobIdExtractor(false);
            assertThat(extractor.isJobLevelSupported()).isFalse();
            assertThat(extractor.extract(new FakeTask(7L))).isEqualTo(JobIdExtractor.NO_JOB);
        }
    }

    @Nested
    @DisplayName("能力位开启时")
    class CapabilityEnabled {

        @Test
        @DisplayName("应直读宿主 task 的 jobID 字段")
        void shouldExtractJobIdFromField() {
            final JobIdExtractor extractor = new JobIdExtractor(true);
            assertThat(extractor.extract(new FakeTask(42L))).isEqualTo(42L);
            assertThat(extractor.extract(new FakeTask(0L))).isEqualTo(0L);
        }

        @Test
        @DisplayName("应沿继承链解析父类声明的 jobID 字段")
        void shouldExtractFromSuperclassField() {
            final JobIdExtractor extractor = new JobIdExtractor(true);
            assertThat(extractor.extract(new SubTask(99L))).isEqualTo(99L);
        }

        @Test
        @DisplayName("无 jobID 字段时应返回 NO_JOB（降级信号，不猜测）")
        void shouldReturnNoJobWhenFieldAbsent() {
            final JobIdExtractor extractor = new JobIdExtractor(true);
            assertThat(extractor.extract(new NoJobTask())).isEqualTo(JobIdExtractor.NO_JOB);
        }

        @Test
        @DisplayName("null task 应返回 NO_JOB")
        void shouldReturnNoJobForNullTask() {
            final JobIdExtractor extractor = new JobIdExtractor(true);
            assertThat(extractor.extract(null)).isEqualTo(JobIdExtractor.NO_JOB);
        }
    }
}