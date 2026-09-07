package com.lingframe.agent.adapter;

import com.lingframe.agent.adapter.FailureClassifier.HitLevel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 熔断失败判定分类器单测。
 * <p>
 * 验收覆盖：
 * <ol>
 *   <li>配置可达类型（IOException/SQLException/TimeoutException 家族）在任意消息文本下计数正确；</li>
 *   <li>显式排除的业务异常不计入，优先级高于下游判定；</li>
 *   <li>默认配置分类结果与现状逐例一致（黄金样本回归）；</li>
 *   <li>决策 100% 可观测（HitLevel 可追溯）；回滚 {@code classifier-enabled=false} 直回原启发式。</li>
 * </ol>
 */
@DisplayName("熔断失败判定分类器")
class FailureClassifierTest {

    /** 自研 connector 运行时异常：非 JDK 家族、message 亦无既有启发式关键字，仅显式配置层 pattern 可识别。 */
    static final class MyConnectorDownException extends RuntimeException {
        MyConnectorDownException(String message) {
            super(message);
        }
    }

    /** 业务数据异常：message 故意含既有启发式误判词，须经显式配置层反向排除。 */
    static final class DataParseException extends RuntimeException {
        DataParseException(String message) {
            super(message);
        }
    }

    private static FailureClassifier classifier(boolean enabled, List<String> downstream, List<String> business) {
        return new FailureClassifier(enabled, downstream, business, 1);
    }

    private static FailureClassifier defaults() {
        return classifier(true, Collections.emptyList(), Collections.emptyList());
    }

    private static HitLevel level(Throwable error) {
        return defaults().classify(error);
    }

    /** 可达类型（IOException/SQLException/TimeoutException 家族）在任意消息文本下计数正确。 */
    @Test
    @DisplayName("类型契约在任意消息文本下命中（含 cause 链）")
    void typeContractMatchesRegardlessOfMessage() {
        // IOException 子类 + 纯业务文本（无任何关键字）：仍命中类型契约
        assertThat(level(new IOException("offset out of range"))).isEqualTo(HitLevel.TYPE_CONTRACT);
        assertThat(level(new SQLException("result set already closed"))).isEqualTo(HitLevel.TYPE_CONTRACT);
        assertThat(level(new TimeoutException("wait exceeded"))).isEqualTo(HitLevel.TYPE_CONTRACT);
        // cause 链任一节点命中即定级
        assertThat(level(new RuntimeException("opaque wrapper", new ConnectException("refused"))))
                .isEqualTo(HitLevel.TYPE_CONTRACT);
        // 下游判定为正
        assertThat(defaults().isDownstreamAvailabilityFailure(new IOException("offset out of range"))).isTrue();
    }

    /** 显式排除的业务异常不计入，优先级高于下游判定。 */
    @Test
    @DisplayName("business-exceptions-patterns 反向排除优先于下游判定")
    void businessExclusionWinsOverDownstream() {
        // 同一异常 message 含既有启发式误判词（connection refused），但显式配置层反向排除命中 → 业务异常
        final DataParseException businessError = new DataParseException("connection refused");
        final FailureClassifier c = classifier(true,
                Collections.emptyList(), Collections.singletonList(".*DataParse.*"));

        assertThat(c.classify(businessError)).isEqualTo(HitLevel.BUSINESS_EXCLUDED);
        assertThat(c.isDownstreamAvailabilityFailure(businessError)).isFalse();
        // 反向排除沿 cause 链生效：包装层业务异常整链排除
        final RuntimeException wrapped = new RuntimeException("opaque", businessError);
        assertThat(c.classify(wrapped)).isEqualTo(HitLevel.BUSINESS_EXCLUDED);
        assertThat(c.isDownstreamAvailabilityFailure(wrapped)).isFalse();
    }

    /** 默认配置分类结果与既有实现逐例一致（黄金样本回归）。 */
    @Test
    @DisplayName("默认配置与既有启发式逐例一致（黄金样本回归）")
    void goldenRegressionMatchesLegacyHeuristic() {
        // 下游样本：type instanceof 命中
        assertDownstream(new IOException("data parse error"));
        // 下游样本：关键字启发式命中
        assertDownstream(new RuntimeException("connection refused"));
        assertDownstream(new RuntimeException("broken pipe"));
        assertDownstream(new RuntimeException("no route to host"));
        assertDownstream(new RuntimeException("timed out"));
        // 下游样本：cause 链关键字命中
        assertDownstream(new RuntimeException("opaque", new RuntimeException("unknown host")));
        // 非下游样本：普通业务异常 / 未知消息不计入
        assertNotDownstream(new IllegalArgumentException("bad schema"));
        assertNotDownstream(new RuntimeException("some random failure"));
        assertNotDownstream(new NullPointerException());
        assertNotDownstream(new RuntimeException());
    }

    /** 决策可观测——classify 返回可追溯的 HitLevel。 */
    @Test
    @DisplayName("分类决策经 HitLevel 100% 可观测")
    void decisionIsObservableViaHitLevel() {
        // 类型契约命中
        assertThat(level(new ConnectException("refused"))).isEqualTo(HitLevel.TYPE_CONTRACT);
        // 兜底启发式命中
        assertThat(level(new RuntimeException("connection refused"))).isEqualTo(HitLevel.HEURISTIC);
        // 显式配置层·正向包含命中（自研 connector FQCN 正则）
        final FailureClassifier withDownstream = classifier(true,
                Collections.singletonList(".*MyConnectorDown.*"), Collections.emptyList());
        assertThat(withDownstream.classify(new MyConnectorDownException("internal middleware down")))
                .isEqualTo(HitLevel.PATTERN_DOWNSTREAM);
        assertThat(withDownstream.isDownstreamAvailabilityFailure(new MyConnectorDownException("internal middleware down")))
                .isTrue();
        // 未命中
        assertThat(level(new IllegalArgumentException("bad schema"))).isEqualTo(HitLevel.NONE);
        assertThat(defaults().classify(null)).isEqualTo(HitLevel.NONE);
    }

    /** 回滚——{@code classifier-enabled=false} 跳过显式配置层直回原启发式。 */
    @Test
    @DisplayName("回滚开关跳过显式配置层，行为与既有实现一致")
    void rollbackSkipsLayer2WhenDisabled() {
        final FailureClassifier disabled = classifier(false,
                Collections.singletonList(".*MyConnectorDown.*"),
                Collections.singletonList(".*DataParse.*"));

        // 仅显式配置层 pattern 可识别的自定义异常：回退后 NONE（旧实现同样不认识）
        assertThat(disabled.classify(new MyConnectorDownException("internal middleware down")))
                .isEqualTo(HitLevel.NONE);
        assertThat(disabled.isDownstreamAvailabilityFailure(new MyConnectorDownException("internal middleware down")))
                .isFalse();
        // 业务排除失效：message 含误判词但 pattern 关闭 → 落回启发式（与旧实现一致）
        assertThat(disabled.classify(new DataParseException("connection refused")))
                .isEqualTo(HitLevel.HEURISTIC);
        // 类型契约 / 关键字启发式仍生效
        assertThat(disabled.classify(new IOException("whatever"))).isEqualTo(HitLevel.TYPE_CONTRACT);
        assertThat(disabled.classify(new RuntimeException("connection refused")))
                .isEqualTo(HitLevel.HEURISTIC);
    }

    /** 非法正则不阻断分类：跳过并告警，其余 pattern 正常生效。 */
    @Test
    @DisplayName("健壮性：非法正则跳过不阻断，合法 pattern 仍生效")
    void invalidPatternSkippedWithoutBlocking() {
        final FailureClassifier c = classifier(true,
                Arrays.asList("[invalid", ".*MyConnectorDown.*"), Collections.emptyList());
        assertThat(c.classify(new MyConnectorDownException("internal middleware down")))
                .isEqualTo(HitLevel.PATTERN_DOWNSTREAM);
    }

    private static void assertDownstream(Throwable error) {
        assertThat(defaults().isDownstreamAvailabilityFailure(error)).isTrue();
    }

    private static void assertNotDownstream(Throwable error) {
        assertThat(defaults().isDownstreamAvailabilityFailure(error)).isFalse();
    }
}
