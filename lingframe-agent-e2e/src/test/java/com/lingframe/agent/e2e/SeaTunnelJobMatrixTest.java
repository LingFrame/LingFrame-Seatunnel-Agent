package com.lingframe.agent.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SeaTunnelJobMatrixTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void kafkaIdsAreIsolatedAcrossSubmissionsAndOperators() throws Exception {
        final String job = "{\"source\":[{\"plugin_name\":\"Kafka\",\"topic\":\"in\","
                + "\"kafka.config\":{\"client.id\":\"custom\",\"fetch.min.bytes\":1024}}],"
                + "\"sink\":[{\"plugin_name\":\"Kafka\",\"topic\":\"out\"},{\"plugin_name\":\"Kafka\"}]}";
        final Set<String> ids = new HashSet<>();
        for (String submission : new String[] {"first", "second"}) {
            final JsonNode config = mapper.readTree(SeaTunnelJobMatrix.withKafkaClientIds(job, submission));
            for (String role : new String[] {"source", "sink"}) {
                for (JsonNode node : config.path(role)) {
                    assertTrue(ids.add(node.path("kafka.config").path("client.id").asText()));
                }
            }
            assertEquals(1024, config.path("source").get(0).path("kafka.config").path("fetch.min.bytes").asInt());
            assertEquals("custom-" + submission + "-source-0",
                    config.path("source").get(0).path("kafka.config").path("client.id").asText());
            assertEquals("in", config.path("source").get(0).path("topic").asText());
            assertEquals("out", config.path("sink").get(0).path("topic").asText());
        }
        assertEquals(6, ids.size());
    }

    @Test
    void nonKafkaConfigIsUnchanged() throws Exception {
        final String job = "{\"source\":[{\"plugin_name\":\"FakeSource\"}],"
                + "\"sink\":[{\"plugin_name\":\"Console\"}]}";
        assertEquals(mapper.readTree(job), mapper.readTree(SeaTunnelJobMatrix.withKafkaClientIds(job, "id")));
    }

    @Test
    void validatesStandardListsAndLegacyStrings() {
        for (String input : new String[] {"[\"input\"]", "\"input\""}) {
            SeaTunnelJobMatrix.validateJobConfig("valid", linkedJob(input));
        }
        for (String input : new String[] {"[]", "[\"input\",\"missing\"]", "null", "42"}) {
            assertThrows(IllegalStateException.class,
                    () -> SeaTunnelJobMatrix.validateJobConfig("invalid", linkedJob(input)));
        }
    }

    @Test
    void bundledMatrixRemainsValid() throws Exception {
        assertEquals(15, SeaTunnelJobMatrix.buildOrthogonalMatrixJobConfigs().size());
    }

    private String linkedJob(String input) {
        return "{\"source\":[{\"plugin_name\":\"FakeSource\",\"plugin_output\":\"input\"}],"
                + "\"transform\":[{\"plugin_name\":\"Sql\",\"plugin_input\":" + input
                + ",\"plugin_output\":\"output\"}],"
                + "\"sink\":[{\"plugin_name\":\"Console\",\"plugin_input\":[\"output\"]}]}";
    }
}
