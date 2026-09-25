package com.lingframe.agent.e2e;

import java.util.List;

/** Pure parser for jmap -clstats output; kept independent from Docker and JUnit. */
final class ClassLoaderStatsParser {
    private ClassLoaderStatsParser() {
    }

    static ClassLoaderStatsResult parse(List<String> lines) {
        final ClassLoaderStatsResult result = new ClassLoaderStatsResult();
        for (String line : lines) {
            if (line == null || line.isEmpty() || line.startsWith("class_loader")
                    || line.contains("total") || line.startsWith("finding")
                    || line.startsWith("computing") || line.startsWith("please")
                    || line.startsWith("Attaching") || line.startsWith("Debugger")
                    || line.startsWith("Server compiler") || line.startsWith("JVM version")) {
                continue;
            }
            String[] columns = line.split("\\t");
            if (columns.length < 5) {
                columns = line.split("\\s+");
            }
            if (columns.length < 5) {
                continue;
            }
            final String classLoaderRef = columns[0].trim();
            final long classes;
            try {
                classes = Long.parseLong(columns[1].trim());
            } catch (NumberFormatException ignored) {
                continue;
            }
            final boolean alive = "live".equalsIgnoreCase(columns[4].trim());
            final String type = columns.length > 5 ? columns[5].trim() : "";
            result.parsedLines++;

            final boolean bootstrap = "<bootstrap>".equals(classLoaderRef)
                    || "<bootstrap>".equals(type)
                    || type.contains("bootstrap")
                    || type.contains("<internal>")
                    || "null".equals(classLoaderRef)
                    || classLoaderRef.matches("0x0+");
            final boolean seaTunnelChild = type.contains("SeaTunnelChildFirstClassLoader")
                    || line.contains("SeaTunnelChildFirstClassLoader");
            final boolean app = type.contains("AppClassLoader") && !type.contains("ExtClassLoader");
            final boolean ext = type.contains("ExtClassLoader");

            if (bootstrap) {
                result.bootstrapClasses = classes;
            } else if (seaTunnelChild) {
                result.subClTotalClasses += classes;
                if (alive) {
                    result.subClAlive++;
                } else {
                    result.subClDead++;
                }
            } else if (app) {
                result.appClasses = classes;
            } else {
                result.otherClasses += classes;
                if (alive) {
                    result.otherAlive++;
                } else {
                    result.otherDead++;
                }
            }
        }
        return result;
    }
}
