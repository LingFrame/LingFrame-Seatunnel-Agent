package com.lingframe.agent.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/** Builds and validates the SeaTunnel job matrix used by the Metaspace audit. */
final class SeaTunnelJobMatrix {
    private static final Logger log = LoggerFactory.getLogger(SeaTunnelJobMatrix.class);

    private SeaTunnelJobMatrix() {
    }

    static String applyTopicPrefix(String jobConfig, String topicPrefix) {
        if (topicPrefix == null || topicPrefix.isEmpty()) {
            return jobConfig;
        }
        return jobConfig.replace("test-topic-1", topicPrefix + "test-topic-1")
                         .replace("test-topic-2", topicPrefix + "test-topic-2");
    }


    static List<String> buildOrthogonalMatrixJobConfigs() throws IOException {
        final List<String> jobs = new ArrayList<>();
        final String externalDir = System.getProperty("lingframe.test.job.dir");
        final Path dir;
        if (externalDir != null && !externalDir.isEmpty()) {
            dir = Paths.get(externalDir);
            log.info("Loading job configs from external directory: {}", externalDir);
        } else {
            final URL dirUrl = SeaTunnelJobMatrix.class.getClassLoader().getResource("e2e-jobs");
            if (dirUrl == null) {
                throw new IllegalStateException("Default e2e-jobs/ directory not found on classpath");
            }
            try {
                dir = Paths.get(dirUrl.toURI());
            } catch (Exception e) {
                throw new IllegalStateException("Invalid e2e-jobs/ directory URL: " + dirUrl, e);
            }
            log.info("Loading job configs from default classpath directory: e2e-jobs/");
        }
        try (Stream<Path> stream = Files.list(dir)) {
            stream.filter(p -> p.toString().endsWith(".json"))
                    .sorted()
                    .forEach(p -> {
                        try {
                            final String content = new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
                            validateJobConfig(p.getFileName().toString(), content);
                            jobs.add(content);
                            log.info("  Loaded: {}", p.getFileName());
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to read job config: " + p, e);
                        }
                    });
        }
        if (jobs.isEmpty()) {
            throw new IllegalStateException("No .json job configs found in: " + dir);
        }
        log.info("Total {} job configs loaded.", jobs.size());
        return jobs;
    }

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    /**
     * 校验作业配置 JSON 的结构完整性与链路连通性。
     * <p>
     * 校验项：
     * <ol>
     *   <li>JSON 格式合法（能解析为 JsonNode）</li>
     *   <li>source / sink 数组非空，每个元素有 plugin_name</li>
     *   <li>transform 若存在，每个元素有 plugin_name / plugin_input / plugin_output</li>
     *   <li>链路连通性：sink.plugin_input 必须能从 source.plugin_output 或 transform.plugin_output 中找到匹配</li>
     * </ol>
     * 校验失败时抛 IllegalStateException，指明文件名和具体问题。
     */
    static void validateJobConfig(String fileName, String jsonContent) {
        final JsonNode root;
        try {
            root = JSON_MAPPER.readTree(jsonContent);
        } catch (Exception e) {
            throw new IllegalStateException("Invalid JSON syntax in " + fileName + ": " + e.getMessage(), e);
        }
        final JsonNode source = root.path("source");
        if (!source.isArray() || source.isEmpty()) {
            throw new IllegalStateException("Missing or empty 'source' array in " + fileName);
        }
        final JsonNode sink = root.path("sink");
        if (!sink.isArray() || sink.isEmpty()) {
            throw new IllegalStateException("Missing or empty 'sink' array in " + fileName);
        }
        final Set<String> outputs = new HashSet<>();
        for (JsonNode node : source) {
            final String pluginName = node.path("plugin_name").asText("");
            if (pluginName.isEmpty()) {
                throw new IllegalStateException("Source entry missing 'plugin_name' in " + fileName);
            }
            final String output = node.path("plugin_output").asText("");
            if (!output.isEmpty()) {
                outputs.add(output);
            }
        }
        final JsonNode transform = root.path("transform");
        if (transform.isArray()) {
            for (JsonNode node : transform) {
                final String pluginName = node.path("plugin_name").asText("");
                if (pluginName.isEmpty()) {
                    throw new IllegalStateException("Transform entry missing 'plugin_name' in " + fileName);
                }
                final String input = node.path("plugin_input").asText("");
                final String output = node.path("plugin_output").asText("");
                if (input.isEmpty() || output.isEmpty()) {
                    throw new IllegalStateException("Transform entry missing 'plugin_input' or 'plugin_output' in " + fileName);
                }
                if (!outputs.contains(input)) {
                    throw new IllegalStateException(
                        "Transform plugin_input '" + input + "' has no matching upstream plugin_output in " + fileName);
                }
                outputs.add(output);
            }
        }
        for (JsonNode node : sink) {
            final String pluginName = node.path("plugin_name").asText("");
            if (pluginName.isEmpty()) {
                throw new IllegalStateException("Sink entry missing 'plugin_name' in " + fileName);
            }
            final String input = node.path("plugin_input").asText("");
            if (!input.isEmpty() && !outputs.contains(input)) {
                throw new IllegalStateException(
                    "Sink plugin_input '" + input + "' has no matching upstream plugin_output in " + fileName);
            }
        }
        log.info("  Validated: {}", fileName);
    }


}
