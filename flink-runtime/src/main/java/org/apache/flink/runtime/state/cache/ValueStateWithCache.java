package org.apache.flink.runtime.state.cache;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.RunnableWithException;

import java.io.IOException;
import java.util.Map;

public class ValueStateWithCache<K, N, V> implements ValueState<V>, StateWithCache<K> {
    private static final long NO_CHECKPOINT_ID = -1;
    private final N namespace;
    private final TypeSerializer<N> namespaceSerializer;
    private final ValueStateDescriptor<V> cacheStateDescriptor;
    private final ValueState<V> state;
    private final ValueState<V> stateForCache;

    private final KeyedStateBackend<K> keyedStateBackend;

    private final KeyedStateBackend<K> keyedStateBackendForCache;

    private LinkedHashMapLRUCache<K, V> lruCache;

    private K currentKey;

    private long currentlyReferencingCheckpointID;

    public ValueStateWithCache(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            ValueStateDescriptor<V> cacheStateDescriptor,
            ValueState<V> state,
            ValueState<V> stateForCache,
            KeyedStateBackend<K> keyedStateBackend,
            KeyedStateBackend<K> keyedStateBackendForCache,
            int keySize)
            throws Exception {
        this.namespace = namespace;
        this.namespaceSerializer = namespaceSerializer;
        this.cacheStateDescriptor = cacheStateDescriptor;
        this.state = state;
        this.stateForCache = stateForCache;
        this.keyedStateBackend = keyedStateBackend;
        this.keyedStateBackendForCache = keyedStateBackendForCache;
        this.lruCache = new LinkedHashMapLRUCache<>(keySize, this::removalCallback);
        this.currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;

        keyedStateBackendForCache.applyToAllKeys(
                namespace,
                namespaceSerializer,
                cacheStateDescriptor,
                (key, cacheState) -> lruCache.put(key, cacheState.value()));
    }

    @Override
    public V value() throws IOException {
        V currentValue = lruCache.get(currentKey);
        if (currentValue == null) {
            keyedStateBackend.setCurrentKey(currentKey);
            currentValue = state.value();
            if (currentlyReferencingCheckpointID != NO_CHECKPOINT_ID) {
                currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;
                lruCache = lruCache.clone();
            }
            lruCache.put(currentKey, currentValue);
        }
        return currentValue;
    }

    @Override
    public void update(V value) throws IOException {
        if (currentlyReferencingCheckpointID != NO_CHECKPOINT_ID) {
            currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;
            lruCache = lruCache.clone();
        }
        lruCache.put(currentKey, value);
    }

    @Override
    public final void setCurrentKey(K key) {
        currentKey = key;
    }

    @Override
    public RunnableWithException notifyLocalSnapshotStarted(long checkpointId) throws Exception {
        // TODO: verify this when max concurrent checkpoint > 1
        Preconditions.checkState(currentlyReferencingCheckpointID == NO_CHECKPOINT_ID);
        currentlyReferencingCheckpointID = checkpointId;

        LinkedHashMapLRUCache<K, V> snapshotCache = lruCache.clone();

        return () -> {
            keyedStateBackendForCache.applyToAllKeys(
                    namespace,
                    namespaceSerializer,
                    cacheStateDescriptor,
                    (key, state) -> state.clear()
            );
            // for (Map.Entry<K, V> entry: lruCache.entrySet()) {
            //     keyedStateBackendForCache.setCurrentKey(entry.getKey());
            //     stateForCache.update(entry.getValue());
            // }
            for (Map.Entry<K, V> entry : snapshotCache.entrySet()) {
                keyedStateBackendForCache.setCurrentKey(entry.getKey());
                stateForCache.update(entry.getValue());
            }
        };
    }

    @Override
    public void notifyLocalSnapshotFinished(long checkpointId) {
        if (currentlyReferencingCheckpointID == checkpointId) {
            currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;
        }
    }

    @Override
    public void clear() {
        if (currentlyReferencingCheckpointID != NO_CHECKPOINT_ID) {
            currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;
            lruCache = lruCache.clone();
        }
        lruCache.put(currentKey, null);
    }

    static <K, N, V> ValueStateWithCache<K, N, V> getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer,
            ValueStateDescriptor<V> stateDescriptor,
            CheckpointableKeyedStateBackend<K> backend,
            CheckpointableKeyedStateBackend<K> backendForCache,
            int keySize)
            throws Exception {
        ValueState<V> state = backend.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
        ValueState<V> stateForCache =
                backendForCache.getOrCreateKeyedState(
                        namespaceSerializer, stateDescriptor);
        return new ValueStateWithCache<>(
                (N) VoidNamespace.INSTANCE,
                namespaceSerializer,
                stateDescriptor,
                state,
                stateForCache,
                backend,
                backendForCache,
                keySize);
    }

    static <K, N, V> ValueStateWithCache<K, N, V> getPartitionedState(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            ValueStateDescriptor<V> stateDescriptor,
            CheckpointableKeyedStateBackend<K> backend,
            CheckpointableKeyedStateBackend<K> backendForCache,
            int keySize) throws Exception {
        ValueState<V> state = backend.getPartitionedState(namespace, namespaceSerializer, stateDescriptor);
        ValueState<V> stateForCache =
                backendForCache.getPartitionedState(
                        namespace, namespaceSerializer, stateDescriptor);
        return new ValueStateWithCache<>(
                namespace,
                namespaceSerializer,
                stateDescriptor,
                state,
                stateForCache,
                backend,
                backendForCache,
                keySize);
    }

    private void removalCallback(K keyToRemove, V value) {
        keyedStateBackend.setCurrentKey(keyToRemove);
        try {
            state.update(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
