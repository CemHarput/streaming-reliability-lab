package io.github.cemharput.streamlab.bench;

import io.github.cemharput.streamlab.bench.ProcessSpecs.ProcessSpec;
import java.io.IOException;

/** Starts a child process from a spec. */
public interface ProcessLauncher {

    ChildProcess launch(ProcessSpec spec) throws IOException;
}
