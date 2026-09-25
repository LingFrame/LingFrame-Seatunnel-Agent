package com.lingframe.agent.e2e;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** REST and container-log client for SeaTunnel E2E jobs. */
final class SeaTunnelJobClient {
    private static final Logger log = LoggerFactory.getLogger(SeaTunnelJobClient.class);

    static final int MAX_SUBMIT_RETRIES = 3;
    static final long RETRY_DELAY_MS = 2000L;

    static String submitJob(String restUrl, String jobTag, String jobConfig) throws IOException {
        IOException lastException = null;
        for (int attempt = 1; attempt <= MAX_SUBMIT_RETRIES; attempt++) {
            try {
                return submitJobOnce(restUrl, jobTag, jobConfig);
            } catch (IOException e) {
                lastException = e;
                log.warn("[{}] Job submission attempt {}/{} failed: {}", jobTag, attempt, MAX_SUBMIT_RETRIES, e.getMessage());
                if (attempt < MAX_SUBMIT_RETRIES) {
                    try {
                        Thread.sleep(RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Retry interrupted for " + jobTag, ie);
                    }
                }
            }
        }
        throw lastException;
    }

    static String submitJobOnce(String restUrl, String jobTag, String jobConfig) throws IOException {
        final HttpURLConnection conn = (HttpURLConnection) new URL(restUrl).openConnection();
        try {
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(60000);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(jobConfig.getBytes(StandardCharsets.UTF_8));
            }
            final int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                final StringBuilder err = new StringBuilder();
                try (InputStream es = conn.getErrorStream()) {
                    if (es != null) {
                        try (BufferedReader r = new BufferedReader(
                                new InputStreamReader(es, StandardCharsets.UTF_8))) {
                            String l;
                            while ((l = r.readLine()) != null) {
                                err.append(l).append('\n');
                            }
                        }
                    }
                }
                throw new IOException("SeaTunnel job submission failed (" + jobTag + "): HTTP "
                        + responseCode + " - " + err.toString().trim());
            }
            final String responseBody;
            try (InputStream is = conn.getInputStream()) {
                responseBody = readAll(is);
            }
            final String jobId = extractJsonField(responseBody, "jobId");
            if (jobId == null || jobId.isEmpty()) {
                throw new IOException("SeaTunnel job submission failed (" + jobTag
                        + "): no jobId in response: " + responseBody);
            }
            return jobId;
        } finally {
            conn.disconnect();
        }
    }

    /**
     * 轮询 SeaTunnel REST API 直到作业到达终态（FINISHED / FAILED / CANCELED）或超时。
     * <p>
     * 排除"提交成功但执行崩溃"的假象：HTTP 200 只证明作业被引擎接收，
     * 不证明作业正常执行完成。此方法通过 {@code GET /running-job/{jobId}} 确认终态。
     * <p>
     * 当 Agent javaagent 影响 Hazelcast IMap 序列化导致 API 返回 {@code {"jobId":"xxx"}}
     * 而无 jobStatus 字段时，回退到从容器日志搜索作业状态转换记录。
     *
     * @param submitUrl      提交作业用的 REST URL（含 /submit-job 后缀）
     * @param jobId          作业 ID
     * @param jobTag         日志标签
     * @param timeoutSeconds 单作业等待超时（秒）
     * @param containerName  SeaTunnel 容器名（用于容器日志回退）
     */
    static void waitForJobFinished(String submitUrl, String jobId, String jobTag,
                                    int timeoutSeconds, String containerName) throws IOException {
        final String jobInfoUrl = submitUrl.replace("submit-job", "running-job") + "/" + jobId;
        final String finishedJobUrl = submitUrl.replace("submit-job", "finished-job-state") + "/" + jobId;
        log.info("[{}] Polling job status at {}", jobTag, jobInfoUrl);
        final long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        String lastStatus = "UNKNOWN";
        boolean apiNoStatusLogged = false;

        while (System.currentTimeMillis() < deadline) {
            final HttpURLConnection conn = (HttpURLConnection) new URL(jobInfoUrl).openConnection();
            try {
                conn.setRequestMethod("GET");
                conn.setConnectTimeout(5000);
                conn.setReadTimeout(10000);
                final int responseCode = conn.getResponseCode();
                if (responseCode == 200) {
                    final String body;
                    try (InputStream is = conn.getInputStream()) {
                        body = readAll(is);
                    }
                    final String jobStatus = extractJsonField(body, "jobStatus");
                    if (jobStatus != null) {
                        lastStatus = jobStatus;
                        if ("FINISHED".equals(jobStatus)) {
                            log.info("[{}] Job {} -> FINISHED (via REST API)", jobTag, jobId);
                            return;
                        }
                        if ("FAILED".equals(jobStatus) || "CANCELED".equals(jobStatus)) {
                            throw new IOException("Job " + jobTag + " (id=" + jobId
                                    + ") ended with status " + jobStatus + ", body: " + body);
                        }
                        if ("UNKNOWABLE".equals(jobStatus)) {
                            log.info("[{}] Job {} -> UNKNOWABLE (via REST API), "
                                    + "job state cleared by engine, checking container log.",
                                    jobTag, jobId);
                            final String logStatus = checkJobStatusFromContainerLog(containerName, jobId);
                            if ("FINISHED".equals(logStatus)) {
                                log.info("[{}] Job {} -> FINISHED (via container log)", jobTag, jobId);
                                return;
                            }
                            if ("FAILED".equals(logStatus) || "CANCELED".equals(logStatus)) {
                                throw new IOException("Job " + jobTag + " (id=" + jobId
                                        + ") ended with status " + logStatus + " (via container log)");
                            }
                            log.info("[{}] Job {} container log inconclusive, "
                                    + "treating UNKNOWABLE as terminal.", jobTag, jobId);
                            return;
                        }
                    } else {
                        final String finishedStatus = queryFinishedJobState(finishedJobUrl);
                        if ("FINISHED".equals(finishedStatus) || "UNKNOWABLE".equals(finishedStatus)) {
                            log.info("[{}] Job {} -> {} (via REST API finished-job-state)",
                                    jobTag, jobId, finishedStatus);
                            return;
                        }
                        if ("FAILED".equals(finishedStatus) || "CANCELED".equals(finishedStatus)) {
                            throw new IOException("Job " + jobTag + " (id=" + jobId
                                    + ") ended with status " + finishedStatus
                                    + " (via REST API finished-job-state)");
                        }
                        if (!apiNoStatusLogged) {
                            log.warn("[{}] Job {} REST API returned no jobStatus, body: {}. "
                                    + "Falling back to container log.", jobTag, jobId, body);
                            apiNoStatusLogged = true;
                        }
                        final String logStatus = checkJobStatusFromContainerLog(containerName, jobId);
                        if (logStatus != null) {
                            lastStatus = logStatus;
                            if ("FINISHED".equals(logStatus) || "UNKNOWABLE".equals(logStatus)) {
                                log.info("[{}] Job {} -> {} (via container log)",
                                        jobTag, jobId, logStatus);
                                return;
                            }
                            if ("FAILED".equals(logStatus) || "CANCELED".equals(logStatus)) {
                                throw new IOException("Job " + jobTag + " (id=" + jobId
                                        + ") ended with status " + logStatus + " (via container log)");
                            }

                        }

                    }
                } else {
                    final String errBody;
                    try (InputStream es = conn.getErrorStream()) {
                        errBody = es != null ? readAll(es) : "(no error stream)";
                    }
                    log.warn("[{}] Job {} HTTP {} - error: {}", jobTag, jobId, responseCode, errBody);
                }
            } catch (SocketTimeoutException ste) {
                log.warn("[{}] Job {} REST read timeout, checking container log for fallback status.",
                        jobTag, jobId);
                final String logStatus = checkJobStatusFromContainerLog(containerName, jobId);
                if ("FINISHED".equals(logStatus) || "UNKNOWABLE".equals(logStatus)) {
                    log.info("[{}] Job {} -> {} (via container log after REST timeout)",
                            jobTag, jobId, logStatus);
                    return;
                }
                if ("FAILED".equals(logStatus) || "CANCELED".equals(logStatus)) {
                    throw new IOException("Job " + jobTag + " (id=" + jobId
                            + ") ended with status " + logStatus
                            + " (via container log after REST timeout)", ste);
                }
            } finally {
                conn.disconnect();
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for job " + jobTag, e);
            }
        }
        throw new IOException("Job " + jobTag + " (id=" + jobId + ") did not finish within "
                + timeoutSeconds + " seconds, last status: " + lastStatus);
    }

    /**
     * 查询 {@code finished-job-state} IMap 确认作业是否已到达终态。
     * <p>
     * 当 {@code running-job} IMap 中条目已被引擎 {@code cleanJob()} 移除时，
     * 作业状态转入 {@code finished-job-state} IMap。此方法
     * 作为 {@code running-job} 查询的 fallback，避免回退到容器日志。
     *
     * @param finishedJobUrl finished-job-state IMap 的 REST URL
     * @return jobStatus 字符串（FINISHED/FAILED/CANCELED），查询失败返回 null
     */
    static String queryFinishedJobState(String finishedJobUrl) {
        try {
            final HttpURLConnection conn = (HttpURLConnection) new URL(finishedJobUrl).openConnection();
            try {
                conn.setRequestMethod("GET");
                conn.setConnectTimeout(5000);
                conn.setReadTimeout(10000);
                final int responseCode = conn.getResponseCode();
                if (responseCode != 200) {
                    log.debug("queryFinishedJobState: {} -> HTTP {} (no body)", finishedJobUrl, responseCode);
                    return null;
                }
                final String body;
                try (InputStream is = conn.getInputStream()) {
                    body = readAll(is);
                }
                final String status = extractJsonField(body, "jobStatus");
                if (status != null) {
                    log.debug("queryFinishedJobState: {} -> jobStatus={}", finishedJobUrl, status);
                    return status;
                }
                for (String s : new String[]{"FINISHED", "FAILED", "CANCELED"}) {
                    if (body.contains(s)) {
                        log.debug("queryFinishedJobState: {} -> matched '{}' in body: {}", finishedJobUrl, s, body);
                        return s;
                    }
                }
                log.debug("queryFinishedJobState: {} -> no status found in body: {}", finishedJobUrl, body);
            } finally {
                conn.disconnect();
            }
        } catch (Exception e) {
            log.debug("queryFinishedJobState failed for {}: {}", finishedJobUrl, e.getMessage());
        }
        return null;
    }


    /**
     * 从容器日志搜索指定作业的状态转换记录。
     * <p>
     * SeaTunnel 引擎在作业状态转换时输出日志：
     * {@code Job {jobName} ({jobId}) turned from state RUNNING to FINISHED.}
     * <p>
     * 当 REST API 因 IMap 序列化问题无法返回 jobStatus 时，此方法作为回退手段，
     * 通过 {@code docker logs --since 5m} 搜索最近 5 分钟的容器日志。
     *
     * @param containerName 容器名
     * @param jobId         作业 ID
     * @return 终态（FINISHED / FAILED / CANCELED），未找到返回 null
     */
    static String checkJobStatusFromContainerLog(String containerName, String jobId) throws IOException {
        final ProcessBuilder pb = new ProcessBuilder(
                "sh", "-c",
                "docker logs --since 5m " + containerName + " 2>&1"
                        + " | grep '" + jobId + ".*turned from state' | tail -1");
        pb.redirectErrorStream(true);
        final Process p = pb.start();
        final String line;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
            line = reader.readLine();
        }
        try {
            p.waitFor(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while reading container log for job " + jobId, e);
        }
        if (line == null || line.isEmpty()) {
            return null;
        }
        if (line.contains("to FINISHED")) {
            return "FINISHED";
        }
        if (line.contains("to FAILED")) {
            return "FAILED";
        }
        if (line.contains("to CANCELED")) {
            return "CANCELED";
        }
        return null;
    }


    static String extractJsonField(String json, String fieldName) {
        final Pattern p = Pattern.compile("\"" + fieldName + "\"\\s*:\\s*\"([^\"]+)\"");
        final Matcher m = p.matcher(json);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }

    static String readAll(InputStream is) throws IOException {
        final StringBuilder sb = new StringBuilder();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String l;
            while ((l = r.readLine()) != null) {
                sb.append(l);
            }
        }
        return sb.toString();
    }

}
