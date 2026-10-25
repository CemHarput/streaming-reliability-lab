package io.github.cemharput.streamlab.bench.testsupport;

import io.github.cemharput.streamlab.bench.ChildProcess;
import io.github.cemharput.streamlab.bench.ProcessLauncher;
import io.github.cemharput.streamlab.bench.ProcessSpecs.ProcessSpec;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/** Records every spec and returns the child the behaviour function makes for it. Starts nothing real. */
public final class FakeLauncher implements ProcessLauncher {

    private final List<ProcessSpec> specs = Collections.synchronizedList(new ArrayList<>());
    private final List<FakeChildProcess> children = Collections.synchronizedList(new ArrayList<>());
    private final Function<ProcessSpec, FakeChildProcess> behaviour;

    public FakeLauncher(Function<ProcessSpec, FakeChildProcess> behaviour) {
        this.behaviour = behaviour;
    }

    @Override
    public ChildProcess launch(ProcessSpec spec) throws IOException {
        specs.add(spec);
        FakeChildProcess child = behaviour.apply(spec);
        children.add(child);
        return child;
    }

    public List<ProcessSpec> specs() {
        synchronized (specs) {
            return List.copyOf(specs);
        }
    }

    public List<String> names() {
        return specs().stream().map(ProcessSpec::name).toList();
    }

    public List<FakeChildProcess> children() {
        synchronized (children) {
            return List.copyOf(children);
        }
    }
}
