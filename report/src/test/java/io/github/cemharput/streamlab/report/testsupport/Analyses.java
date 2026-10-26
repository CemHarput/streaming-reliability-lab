package io.github.cemharput.streamlab.report.testsupport;

import io.github.cemharput.streamlab.report.InputException;
import io.github.cemharput.streamlab.report.ResultSetReader;
import io.github.cemharput.streamlab.report.RunAssessment;
import java.io.IOException;

/** Builds a fixture and runs the reader and the assessment over it. */
public final class Analyses {

    private Analyses() {
    }

    public static RunAssessment.Analysis of(ResultSetBuilder builder) {
        try {
            return RunAssessment.analyse(ResultSetReader.read(builder.build()));
        } catch (IOException | InputException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
