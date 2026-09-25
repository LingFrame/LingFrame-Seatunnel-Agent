package com.lingframe.agent.e2e;

/** Parsed summary of one jmap -clstats sample. */
final class ClassLoaderStatsResult {
    long bootstrapClasses;
    long appClasses;
    long subClTotalClasses;
    int subClAlive;
    int subClDead;
    long otherClasses;
    int otherAlive;
    int otherDead;
    int parsedLines;
}
