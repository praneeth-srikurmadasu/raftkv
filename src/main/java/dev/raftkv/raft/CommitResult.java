package dev.raftkv.raft;

import java.util.Objects;

import dev.raftkv.common.Bytes;

/**
 * What a successful {@link RaftNode#propose} completes with.
 *
 * @param index  log index the command was committed at
 * @param term   term of that entry
 * @param result what {@link StateMachine#apply} returned for it
 */
public record CommitResult(long index, long term, Bytes result) {

    public CommitResult {
        Objects.requireNonNull(result, "result");
    }
}
