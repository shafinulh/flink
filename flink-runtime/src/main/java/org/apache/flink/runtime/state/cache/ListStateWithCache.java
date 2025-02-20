package org.apache.flink.runtime.state.cache;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.RunnableWithException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ListStateWithCache<K, N, V> implements InternalListState<K, N, V>, StateWithCache<K> {
    private static final long NO_CHECKPOINT_ID = -1;
    private final N namespace;
    private final TypeSerializer<N> namespaceSerializer;
    private final ListStateDescriptor<V> cacheStateDescriptor;
    private final InternalListState<K, N, V> state;
    private final InternalListState<K, N, V> stateForCache;

    private final KeyedStateBackend<K> keyedStateBackend;

    private final KeyedStateBackend<K> keyedStateBackendForCache;

    private LinkedHashMapLRUCache<K, List<V>> lruCache;

    private K currentKey;

    private long currentlyReferencingCheckpointID;

    public ListStateWithCache(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            ListStateDescriptor<V> cacheStateDescriptor,
            InternalListState<K, N, V> state,
            InternalListState<K, N, V> stateForCache,
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
                (key, cacheState) -> {
                    List<V> list = new ArrayList<>();
                    lruCache.put(key, list);
                    cacheState.get().forEach(list::add);
                });
    }

    @Override
    public Iterable<V> get() throws Exception {
        List<V> currentValue = lruCache.get(currentKey);
        if (currentValue == null) {
            keyedStateBackend.setCurrentKey(currentKey);
            Iterable<V> tmpCurrentValue = state.get();
            if (tmpCurrentValue != null) {
                currentValue = new ArrayList<>();
                tmpCurrentValue.forEach(currentValue::add);
            }
            if (currentlyReferencingCheckpointID != NO_CHECKPOINT_ID) {
                currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;
                lruCache = lruCache.clone();
            }
            lruCache.put(currentKey, currentValue);
        }
        return currentValue;
    }

    @Override
    public void add(V value) throws Exception {
        Preconditions.checkNotNull(value);
        if (currentlyReferencingCheckpointID != NO_CHECKPOINT_ID) {
            currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;
            lruCache = lruCache.clone();
        }
        List<V> list = (List<V>) get();
        if (list == null) {
            list = new ArrayList<>();
        }
        list.add(value);
        update(list);
    }

    @Override
    public void update(List<V> values) throws Exception {
        Preconditions.checkNotNull(values);
        for (V value: values) {
            Preconditions.checkNotNull(value);
        }
        if (values != null) {
            if (values.isEmpty()) {
                values = null;
            } else {
                values = new ArrayList<>(values);
            }
        }
        if (currentlyReferencingCheckpointID != NO_CHECKPOINT_ID) {
            currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;
            lruCache = lruCache.clone();
        }
        lruCache.put(currentKey, values);
    }

    @Override
    public void addAll(List<V> values) throws Exception {
        if (currentlyReferencingCheckpointID != NO_CHECKPOINT_ID) {
            currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;
            lruCache = lruCache.clone();
        }
        List<V> list = (List<V>) get();
        if (list == null) {
            list = new ArrayList<>();
        }
        list.addAll(values);
        update(list);
    }

    @Override
    public void clear() {
        if (currentlyReferencingCheckpointID != NO_CHECKPOINT_ID) {
            currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;
            lruCache = lruCache.clone();
        }
        lruCache.put(currentKey, null);
    }

    @Override
    public void setCurrentKey(K key) {
        currentKey = key;
    }

    @Override
    public RunnableWithException notifyLocalSnapshotStarted(long checkpointId) throws Exception {
        // TODO: verify this when max concurrent checkpoint > 1
        Preconditions.checkState(currentlyReferencingCheckpointID == NO_CHECKPOINT_ID);
        currentlyReferencingCheckpointID = checkpointId;
        return () -> {
            keyedStateBackendForCache.applyToAllKeys(
                    namespace,
                    namespaceSerializer,
                    cacheStateDescriptor,
                    (key, state) -> state.clear()
            );
            for (Map.Entry<K, List<V>> entry: lruCache.entrySet()) {
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

    static <K, N, V> ListStateWithCache<K, N, V> getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer,
            ListStateDescriptor<V> stateDescriptor,
            CheckpointableKeyedStateBackend<K> backend,
            CheckpointableKeyedStateBackend<K> backendForCache,
            int keySize)
            throws Exception {
        InternalListState<K, N, V> state = (InternalListState<K, N, V>) backend.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
        InternalListState<K, N, V> stateForCache =
                (InternalListState<K, N, V>) backendForCache.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
        return new ListStateWithCache<>(
                (N) VoidNamespace.INSTANCE,
                namespaceSerializer,
                stateDescriptor,
                state,
                stateForCache,
                backend,
                backendForCache,
                keySize);
    }

    static <K, N, V> ListStateWithCache<K, N, V> getPartitionedState(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            ListStateDescriptor<V> stateDescriptor,
            CheckpointableKeyedStateBackend<K> backend,
            CheckpointableKeyedStateBackend<K> backendForCache,
            int keySize) throws Exception {
        InternalListState<K, N, V> state = (InternalListState<K, N, V>) backend.getPartitionedState(namespace, namespaceSerializer, stateDescriptor);
        InternalListState<K, N, V> stateForCache =
                (InternalListState<K, N, V>) backendForCache.getPartitionedState(
                        namespace, namespaceSerializer, stateDescriptor);
        return new ListStateWithCache<>(
                namespace,
                namespaceSerializer,
                stateDescriptor,
                state,
                stateForCache,
                backend,
                backendForCache,
                keySize);
    }

    private void removalCallback(K keyToRemove, List<V> value) {
        keyedStateBackend.setCurrentKey(keyToRemove);
        try {
            if (value == null) {
                value = new ArrayList<>();
            }
            state.update(value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // try {
        //     state.update(value);
        // } catch (Exception e) {
        //     throw new RuntimeException(e);
        // }
    }

    @Override
    public List<V> getInternal() throws Exception {
        return (List<V>) get();
    }

    @Override
    public void updateInternal(List<V> valueToStore) throws Exception {
        update(valueToStore);
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return stateForCache.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return stateForCache.getNamespaceSerializer();
    }

    @Override
    public TypeSerializer<List<V>> getValueSerializer() {
        return stateForCache.getValueSerializer();
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        state.setCurrentNamespace(namespace);
        stateForCache.setCurrentNamespace(namespace);
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<List<V>> safeValueSerializer) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public StateIncrementalVisitor<K, N, List<V>> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        throw new UnsupportedOperationException();
    }
}
