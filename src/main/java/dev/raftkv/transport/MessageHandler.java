package dev.raftkv.transport;

/** Receives messages addressed to one node. Called on that node's event loop. */
@FunctionalInterface
public interface MessageHandler {

    void onMessage(Envelope envelope);
}
