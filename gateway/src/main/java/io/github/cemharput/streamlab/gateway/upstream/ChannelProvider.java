package io.github.cemharput.streamlab.gateway.upstream;

import io.grpc.Channel;
import io.grpc.ConnectivityState;

public interface ChannelProvider {

    /** Returns a channel for one call; throws {@link IllegalStateException} when not running. */
    Lease acquire();

    ConnectivityState state(boolean requestConnection);

    interface Lease extends AutoCloseable {

        Channel channel();

        /** Shared channels: no-op. */
        @Override
        void close();
    }
}
