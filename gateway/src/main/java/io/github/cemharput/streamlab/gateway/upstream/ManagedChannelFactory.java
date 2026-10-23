package io.github.cemharput.streamlab.gateway.upstream;

import io.grpc.ManagedChannel;

@FunctionalInterface
public interface ManagedChannelFactory {

    ManagedChannel create(String target);
}
