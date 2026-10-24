package io.github.cemharput.streamlab.gateway.upstream;

import io.grpc.Channel;
import io.grpc.ConnectivityState;

public interface ChannelProvider {

    /**
     * Returns the lease of one stream (the channel itself may only be created later, see {@link Lease#channel()});
     * throws {@link IllegalStateException} when not running.
     */
    Lease acquire();

    ConnectivityState state(boolean requestConnection);

    /**
     * The channel binding of one stream. A lease belongs to a single stream. {@link #channel()} may create the
     * channel lazily on its first call and returns the same channel on every later call of the same lease.
     */
    interface Lease extends AutoCloseable {

        Channel channel();

        /**
         * Idempotent and non-blocking; the stream calls it from the one path that wins its terminal claim,
         * possibly while holding a lock. Shared channels: no-op. Per-request channels: starts a graceful shutdown.
         */
        @Override
        void close();
    }
}
