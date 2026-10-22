package io.github.cemharput.streamlab.simulator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.cemharput.streamlab.tokenstream.v1.FaultSpec;
import io.github.cemharput.streamlab.tokenstream.v1.FaultType;
import io.github.cemharput.streamlab.tokenstream.v1.GenerateRequest;
import io.github.cemharput.streamlab.tokenstream.v1.GetStatsRequest;
import io.github.cemharput.streamlab.tokenstream.v1.SimulatorAdminServiceGrpc;
import io.github.cemharput.streamlab.tokenstream.v1.SimulatorStats;
import io.github.cemharput.streamlab.tokenstream.v1.StreamProfile;
import io.github.cemharput.streamlab.tokenstream.v1.TokenChunk;
import io.github.cemharput.streamlab.tokenstream.v1.TokenStreamServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TokenStreamServiceTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private SimulatorServer server;
    private ManagedChannel channel;
    private TokenStreamServiceGrpc.TokenStreamServiceBlockingStub blocking;
    private TokenStreamServiceGrpc.TokenStreamServiceStub async;
    private SimulatorAdminServiceGrpc.SimulatorAdminServiceBlockingStub admin;

    @BeforeEach
    void setUp() throws Exception {
        startServer(Duration.ofSeconds(5));
    }

    @AfterEach
    void tearDown() throws Exception {
        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        server.stop();
    }

    @Test
    void completedStreamHasContiguousSeqAndSingleLastChunk() {
        List<TokenChunk> chunks = collect(request("r-1", 10).build());

        assertThat(chunks).extracting(TokenChunk::getSeq).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertThat(chunks).extracting(TokenChunk::getRequestId).containsOnly("r-1");
        assertThat(chunks).filteredOn(TokenChunk::getLast).extracting(TokenChunk::getSeq).containsExactly(9);

        SimulatorStats stats = awaitIdle();
        assertThat(stats.getCompletedTotal()).isEqualTo(1);
        assertThat(stats.getChunksEmittedTotal()).isEqualTo(10);
    }

    @Test
    void resumedStreamIsByteIdenticalToOriginalSuffix() {
        List<String> full = texts(collect(request("r-1", 10).build()));
        List<String> resumed = texts(collect(request("r-1", 10).setResumeFromSeq(6).build()));

        assertThat(resumed).containsExactlyElementsOf(full.subList(6, 10));
    }

    @Test
    void differentSeedsProduceDifferentContent() {
        List<String> a = texts(collect(request("r-1", 20).setSeed(1).build()));
        List<String> b = texts(collect(request("r-1", 20).setSeed(2).build()));

        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void chunkSizeIsRespected() {
        GenerateRequest request = request("r-1", 3)
                .setProfile(profile(3).setChunkSizeBytes(256))
                .build();

        assertThat(collect(request)).allSatisfy(chunk -> assertThat(chunk.getText().length()).isGreaterThanOrEqualTo(256));
    }

    @Test
    void unavailableFaultDeliversPrefixThenStatus() {
        GenerateRequest request = request("r-1", 10)
                .setFault(FaultSpec.newBuilder().setType(FaultType.FAULT_TYPE_UNAVAILABLE).setAtSeq(4))
                .build();
        List<TokenChunk> received = new ArrayList<>();

        assertThatThrownBy(() -> blocking.generateTokens(request).forEachRemaining(received::add))
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        ex -> assertThat(ex.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE));
        assertThat(received).extracting(TokenChunk::getSeq).containsExactly(0, 1, 2, 3);

        SimulatorStats stats = awaitIdle();
        assertThat(stats.getFaultedTotal()).isEqualTo(1);
        assertThat(stats.getCompletedTotal()).isZero();
    }

    @Test
    void internalFaultBeforeFirstChunk() {
        GenerateRequest request = request("r-1", 5)
                .setFault(FaultSpec.newBuilder().setType(FaultType.FAULT_TYPE_INTERNAL).setAtSeq(0))
                .build();

        assertThatThrownBy(() -> blocking.generateTokens(request).next())
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        ex -> assertThat(ex.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL));
        assertThat(awaitIdle().getFaultedTotal()).isEqualTo(1);
    }

    @Test
    void clientCancelStopsUpstreamGeneration() {
        GenerateRequest request = request("r-1", 1_000)
                .setProfile(profile(1_000).setChunkIntervalMs(5))
                .build();
        CompletableFuture<Status> terminal = new CompletableFuture<>();

        async.generateTokens(request, observer(
                (requestStream, chunk) -> {
                    if (chunk.getSeq() == 2) {
                        requestStream.cancel("client gave up", null);
                    }
                },
                terminal));

        assertThat(terminal.join().getCode()).isEqualTo(Status.Code.CANCELLED);
        SimulatorStats stats = awaitIdle();
        assertThat(stats.getCancelledTotal()).isEqualTo(1);
        assertThat(stats.getCompletedTotal()).isZero();
        assertThat(stats.getChunksEmittedTotal()).isLessThan(1_000);
    }

    @Test
    void deadlineOnHangingStreamReleasesServerState() {
        GenerateRequest request = request("r-1", 10)
                .setFault(FaultSpec.newBuilder().setType(FaultType.FAULT_TYPE_HANG).setAtSeq(3))
                .build();
        List<TokenChunk> received = new ArrayList<>();

        assertThatThrownBy(() -> blocking.withDeadlineAfter(300, TimeUnit.MILLISECONDS)
                .generateTokens(request)
                .forEachRemaining(received::add))
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        ex -> assertThat(ex.getStatus().getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED));
        assertThat(received).extracting(TokenChunk::getSeq).containsExactly(0, 1, 2);

        SimulatorStats stats = awaitIdle();
        assertThat(stats.getCancelledTotal()).isEqualTo(1);
    }

    @Test
    void slowConsumerPausesProducerInsteadOfBuffering() {
        GenerateRequest request = request("r-1", 20).build();
        List<TokenChunk> received = new CopyOnWriteArrayList<>();
        AtomicReference<ClientCallStreamObserver<GenerateRequest>> stream = new AtomicReference<>();
        CompletableFuture<Status> terminal = new CompletableFuture<>();

        async.generateTokens(request, new ClientResponseObserver<GenerateRequest, TokenChunk>() {
            @Override
            public void beforeStart(ClientCallStreamObserver<GenerateRequest> requestStream) {
                requestStream.disableAutoRequestWithInitial(1);
                stream.set(requestStream);
            }

            @Override
            public void onNext(TokenChunk chunk) {
                received.add(chunk);
            }

            @Override
            public void onError(Throwable t) {
                terminal.complete(Status.fromThrowable(t));
            }

            @Override
            public void onCompleted() {
                terminal.complete(Status.OK);
            }
        });

        // The consumer asked for one chunk; the producer must stop and wait.
        await().atMost(TIMEOUT).until(() -> stats().getFlowControlPausesTotal() >= 1);
        assertThat(stats().getChunksEmittedTotal()).isEqualTo(1);
        assertThat(stats().getActiveGenerations()).isEqualTo(1);

        stream.get().request(Integer.MAX_VALUE);

        assertThat(terminal.join().getCode()).isEqualTo(Status.Code.OK);
        assertThat(received).extracting(TokenChunk::getSeq).hasSize(20).isSorted();
        assertThat(awaitIdle().getCompletedTotal()).isEqualTo(1);
    }

    @Test
    void invalidRequestIsRejectedWithoutStartingGeneration() {
        GenerateRequest request = request("r-1", 5).setProfile(profile(0)).build();

        assertThatThrownBy(() -> blocking.generateTokens(request).next())
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        ex -> assertThat(ex.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
        SimulatorStats stats = stats();
        assertThat(stats.getRejectedTotal()).isEqualTo(1);
        assertThat(stats.getStartedTotal()).isZero();
    }

    @Test
    void resumeBeyondEndIsRejected() {
        GenerateRequest request = request("r-1", 5).setResumeFromSeq(5).build();

        assertThatThrownBy(() -> blocking.generateTokens(request).next())
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        ex -> assertThat(ex.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
    }

    @Test
    void shutdownCancelsStreamsThatOutliveTheGracePeriod() throws Exception {
        tearDown();
        startServer(Duration.ofMillis(200));
        GenerateRequest request = request("r-1", 10)
                .setFault(FaultSpec.newBuilder().setType(FaultType.FAULT_TYPE_HANG).setAtSeq(1))
                .build();
        Iterator<TokenChunk> stream = blocking.generateTokens(request);
        assertThat(stream.next().getSeq()).isZero();

        long startedAt = System.nanoTime();
        server.stop();
        Duration stopTook = Duration.ofNanos(System.nanoTime() - startedAt);

        // Bounded shutdown: grace period plus forced cancel, not "wait forever".
        assertThat(stopTook).isLessThan(Duration.ofSeconds(3));
        assertThatThrownBy(stream::next).isInstanceOf(StatusRuntimeException.class);
    }

    private void startServer(Duration grace) throws Exception {
        String name = InProcessServerBuilder.generateName();
        server = SimulatorServer.start(
                InProcessServerBuilder.forName(name),
                new SimulatorConfig(0, grace, 2, RequestLimits.DEFAULT));
        channel = InProcessChannelBuilder.forName(name).build();
        blocking = TokenStreamServiceGrpc.newBlockingStub(channel);
        async = TokenStreamServiceGrpc.newStub(channel);
        admin = SimulatorAdminServiceGrpc.newBlockingStub(channel);
    }

    private List<TokenChunk> collect(GenerateRequest request) {
        List<TokenChunk> chunks = new ArrayList<>();
        blocking.withDeadlineAfter(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .generateTokens(request)
                .forEachRemaining(chunks::add);
        return chunks;
    }

    private SimulatorStats stats() {
        return admin.getStats(GetStatsRequest.getDefaultInstance());
    }

    private SimulatorStats awaitIdle() {
        await().atMost(TIMEOUT).until(() -> stats().getActiveGenerations() == 0);
        return stats();
    }

    private static GenerateRequest.Builder request(String requestId, int chunkCount) {
        return GenerateRequest.newBuilder()
                .setRequestId(requestId)
                .setSeed(42)
                .setProfile(profile(chunkCount));
    }

    private static StreamProfile.Builder profile(int chunkCount) {
        return StreamProfile.newBuilder()
                .setChunkCount(chunkCount)
                .setFirstChunkDelayMs(1)
                .setChunkIntervalMs(1);
    }

    private static List<String> texts(List<TokenChunk> chunks) {
        return chunks.stream().map(TokenChunk::getText).toList();
    }

    private interface ChunkHandler {
        void onChunk(ClientCallStreamObserver<GenerateRequest> requestStream, TokenChunk chunk);
    }

    private static ClientResponseObserver<GenerateRequest, TokenChunk> observer(
            ChunkHandler onChunk, CompletableFuture<Status> terminal) {
        return new ClientResponseObserver<>() {
            private ClientCallStreamObserver<GenerateRequest> requestStream;

            @Override
            public void beforeStart(ClientCallStreamObserver<GenerateRequest> requestStream) {
                this.requestStream = requestStream;
            }

            @Override
            public void onNext(TokenChunk chunk) {
                onChunk.onChunk(requestStream, chunk);
            }

            @Override
            public void onError(Throwable t) {
                terminal.complete(Status.fromThrowable(t));
            }

            @Override
            public void onCompleted() {
                terminal.complete(Status.OK);
            }
        };
    }
}
