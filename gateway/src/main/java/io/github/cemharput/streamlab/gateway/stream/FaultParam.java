package io.github.cemharput.streamlab.gateway.stream;

import io.github.cemharput.streamlab.tokenstream.v1.FaultType;

/** HTTP-facing fault names; mapped to the proto enum. */
public enum FaultParam {
    NONE(FaultType.FAULT_TYPE_NONE),
    UNAVAILABLE(FaultType.FAULT_TYPE_UNAVAILABLE),
    INTERNAL(FaultType.FAULT_TYPE_INTERNAL),
    HANG(FaultType.FAULT_TYPE_HANG);

    private final FaultType protoType;

    FaultParam(FaultType protoType) {
        this.protoType = protoType;
    }

    public FaultType protoType() {
        return protoType;
    }
}
