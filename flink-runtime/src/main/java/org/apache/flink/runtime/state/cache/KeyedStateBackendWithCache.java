package org.apache.flink.runtime.state.cache;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.TestableKeyedStateBackend;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.RunnableWithException;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KeyedStateBackendWithCache<K>
        implements CheckpointableKeyedStateBackend<K>, TestableKeyedStateBackend<K> {
    private final CheckpointableKeyedStateBackend<K> backend;
    private final CheckpointableKeyedStateBackend<K> backendForCache;
    private final int keySize;
    private final Map<String, StateWithCache<K>> states;
    private K currentKey;

    public KeyedStateBackendWithCache(
            CheckpointableKeyedStateBackend<K> backend,
            CheckpointableKeyedStateBackend<K> backendForCache,
            int keySize) {
        this.backend = Preconditions.checkNotNull(backend);
        this.backendForCache = Preconditions.checkNotNull(backendForCache);
        this.keySize = keySize;
        this.states = new HashMap<>();
        this.currentKey = null;
    }

    @Override
    public void setCurrentKey(K newKey) {
        currentKey = newKey;
        for (StateWithCache<K> state: states.values()) {
            state.setCurrentKey(newKey);
        }
    }

    @Override
    public K getCurrentKey() {
        return currentKey;
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return backend.getKeySerializer();
    }

    @Override
    public <N, S extends State, T> void applyToAllKeys(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, T> stateDescriptor,
            KeyedStateFunction<K, S> function)
            throws Exception {
        throw new UnsupportedOperationException();
//        for (StateWithCache state : states.values()) {
//            state.removeOutdatedState();
//        }
//        backendForCache.applyToAllKeys(
//                namespace, namespaceSerializer, stateDescriptor, function);
//        backend.applyToAllKeys(
//                namespace, namespaceSerializer, stateDescriptor, function);
    }

    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        throw new UnsupportedOperationException();
//        Set<K> keysInCache = backendForCache.getKeys(state, namespace).collect(Collectors.toSet());
//        return Stream.concat(
//                keysInCache.stream(),
//                backend.getKeys(state, namespace).filter(k -> !keysInCache.contains(k)));
    }

    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
        throw new UnsupportedOperationException();
//        Set<K> keysInCache =
//                backendForCache
//                        .getKeysAndNamespaces(state)
//                        .map(x -> x.f0)
//                        .collect(Collectors.toSet());
//        return Stream.concat(
//                backendForCache.getKeysAndNamespaces(state),
//                backend.getKeysAndNamespaces(state)
//                        .filter(x -> !keysInCache.contains(x.f0))
//                        .map(x -> (Tuple2<K, N>) x));
    }

    @Override
    public <N, S extends State, T> S getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor)
            throws Exception {
        StateWithCache<K> result;
        if (stateDescriptor instanceof ValueStateDescriptor) {
            result = ValueStateWithCache.getOrCreateKeyedState(
                    namespaceSerializer,
                    (ValueStateDescriptor) stateDescriptor,
                    backend,
                    backendForCache,
                    keySize
            );
        } else if (stateDescriptor instanceof ListStateDescriptor) {
            result = ListStateWithCache.getOrCreateKeyedState(
                    namespaceSerializer,
                    (ListStateDescriptor) stateDescriptor,
                    backend,
                    backendForCache,
                    keySize
            );
        } else if (stateDescriptor instanceof MapStateDescriptor) {
            result = MapStateWithCache.getOrCreateKeyedState(
                    namespaceSerializer,
                    (MapStateDescriptor) stateDescriptor,
                    backend,
                    backendForCache,
                    keySize
            );
        } else {
            throw new UnsupportedOperationException(stateDescriptor.getClass().getCanonicalName());
        }
        if (currentKey != null) {
            result.setCurrentKey(currentKey);
        }
        // TODO: avoid recreating state with cache.
        states.put(stateDescriptor.getName(), result);
        return (S) result;
    }

    @Override
    public <N, S extends State> S getPartitionedState(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, ?> stateDescriptor)
            throws Exception {
        StateWithCache<K> result;
        if (stateDescriptor instanceof ValueStateDescriptor) {
            result = ValueStateWithCache.getPartitionedState(
                    namespace,
                    namespaceSerializer,
                    (ValueStateDescriptor) stateDescriptor,
                    backend,
                    backendForCache,
                    keySize
            );
        } else if (stateDescriptor instanceof ListStateDescriptor) {
            result = ListStateWithCache.getPartitionedState(
                    namespace,
                    namespaceSerializer,
                    (ListStateDescriptor) stateDescriptor,
                    backend,
                    backendForCache,
                    keySize
            );
        } else if (stateDescriptor instanceof MapStateDescriptor) {
            result = MapStateWithCache.getPartitionedState(
                    namespace,
                    namespaceSerializer,
                    (MapStateDescriptor) stateDescriptor,
                    backend,
                    backendForCache,
                    keySize
            );
        } else {
            throw new UnsupportedOperationException(stateDescriptor.getClass().getCanonicalName());
        }
        if (currentKey != null) {
            result.setCurrentKey(currentKey);
        }
        // TODO: avoid recreating state with cache.
        states.put(stateDescriptor.getName(), result);
        return (S) result;
    }

    @Override
    public void dispose() {
        backendForCache.dispose();
        backend.dispose();
    }

    @Override
    public void registerKeySelectionListener(KeySelectionListener<K> listener) {
        backendForCache.registerKeySelectionListener(listener);
        backend.registerKeySelectionListener(listener);
    }

    @Override
    public boolean deregisterKeySelectionListener(KeySelectionListener<K> listener) {
        return backendForCache.deregisterKeySelectionListener(listener)
                && backend.deregisterKeySelectionListener(listener);
    }

    @Nonnull
    @Override
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull
                    StateSnapshotTransformer.StateSnapshotTransformFactory<SEV>
                            snapshotTransformFactory)
            throws Exception {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        // TODO: optimize created queue.
        return backend.create(stateName, byteOrderedElementSerializer);
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return backend.getKeyGroupRange();
    }

    @Nonnull
    @Override
    public SavepointResources<K> savepoint() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        backend.close();
        backendForCache.close();
    }

    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {
        List<RunnableWithException> runnables = new ArrayList<>();
        for (StateWithCache<K> state : states.values()) {
            runnables.add(state.notifyLocalSnapshotStarted(checkpointId));
        }
        return new RunnableFutureWithCache(
                runnables,
                backend,
                backendForCache,
                checkpointId,
                timestamp,
                streamFactory,
                checkpointOptions,
                () -> {
                    for (StateWithCache<K> state : states.values()) {
                        state.notifyLocalSnapshotFinished(checkpointId);
                    }
                }
        );
    }

    @Override
    public int numKeyValueStateEntries() {
        if (!(backend instanceof TestableKeyedStateBackend)
                || !(backendForCache instanceof TestableKeyedStateBackend)) {
            throw new UnsupportedOperationException();
        }
        return ((TestableKeyedStateBackend<?>) backend).numKeyValueStateEntries()
                + ((TestableKeyedStateBackend<?>) backendForCache).numKeyValueStateEntries();
    }
}
