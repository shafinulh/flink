package org.apache.flink.runtime.state.cache;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.RunnableWithException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MapStateWithCache<K, N, UK, UV> implements InternalMapState<K, N, UK, UV>, StateWithCache<K> {
    private static final long NO_CHECKPOINT_ID = -1;
    private final N namespace;
    private final TypeSerializer<N> namespaceSerializer;
    private final MapStateDescriptor<UK, UV> cacheStateDescriptor;
    private final InternalMapState<K, N, UK, UV> state;
    private final InternalMapState<K, N, UK, UV> stateForCache;
    private final KeyedStateBackend<K> keyedStateBackend;
    private final KeyedStateBackend<K> keyedStateBackendForCache;
    private LinkedHashMapLRUCache<K, Map<UK, UV>> lruCache;
    private K currentKey;
    private long currentlyReferencingCheckpointID;

    public MapStateWithCache(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            MapStateDescriptor<UK, UV> cacheStateDescriptor,
            InternalMapState<K, N, UK, UV> state,
            InternalMapState<K, N, UK, UV> stateForCache,
            KeyedStateBackend<K> keyedStateBackend,
            KeyedStateBackend<K> keyedStateBackendForCache,
            int keySize) throws Exception {
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
                    Map<UK, UV> map = new HashMap<>();
                    Iterator<Map.Entry<UK, UV>> iterator = cacheState.iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<UK, UV> entry = iterator.next();
                        map.put(entry.getKey(), entry.getValue());
                    }
                    if (!map.isEmpty()) {
                        lruCache.put(key, map);
                    }
                });
    }

    @Override
    public UV get(UK key) throws Exception {
        Map<UK, UV> map = getMap();
        return map != null ? map.get(key) : null;
    }

    @Override
    public void put(UK key, UV value) throws Exception {
        if (currentlyReferencingCheckpointID != NO_CHECKPOINT_ID) {
            currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;
            lruCache = lruCache.clone();
        }
        Map<UK, UV> map = getMap();
        if (map == null) {
            map = new HashMap<>();
            lruCache.put(currentKey, map);
        }
        map.put(key, value);
    }

    @Override
    public void putAll(Map<UK, UV> map) throws Exception {
        if (currentlyReferencingCheckpointID != NO_CHECKPOINT_ID) {
            currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;
            lruCache = lruCache.clone();
        }
        Map<UK, UV> existing = getMap();
        if (existing == null) {
            existing = new HashMap<>();
            lruCache.put(currentKey, existing);
        }
        existing.putAll(map);
    }

    @Override
    public void remove(UK key) throws Exception {
        if (currentlyReferencingCheckpointID != NO_CHECKPOINT_ID) {
            currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;
            lruCache = lruCache.clone();
        }
        Map<UK, UV> map = getMap();
        if (map != null) {
            map.remove(key);
            if (map.isEmpty()) {
                lruCache.put(currentKey, null);
            }
        }
    }

    @Override
    public boolean contains(UK key) throws Exception {
        Map<UK, UV> map = getMap();
        return map != null && map.containsKey(key);
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        Map<UK, UV> map = getMap();
        return map != null ? map.entrySet() : new ArrayList<>();
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        Map<UK, UV> map = getMap();
        return map != null ? map.keySet() : new ArrayList<>();
    }

    @Override
    public Iterable<UV> values() throws Exception {
        Map<UK, UV> map = getMap();
        return map != null ? map.values() : new ArrayList<>();
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        Map<UK, UV> map = getMap();
        return map != null ? map.entrySet().iterator() : new ArrayList<Map.Entry<UK, UV>>().iterator();
    }

    @Override
    public boolean isEmpty() throws Exception {
        Map<UK, UV> map = getMap();
        return map == null || map.isEmpty();
    }

    @Override
    public void clear() {
        if (currentlyReferencingCheckpointID != NO_CHECKPOINT_ID) {
            currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;
            lruCache = lruCache.clone();
        }
        lruCache.put(currentKey, null);
    }

    private Map<UK, UV> getMap() throws Exception {
        Map<UK, UV> currentValue = lruCache.get(currentKey);
        if (currentValue == null) {
            keyedStateBackend.setCurrentKey(currentKey);
            Iterator<Map.Entry<UK, UV>> iterator = state.iterator();
            if (iterator.hasNext()) {
                currentValue = new HashMap<>();
                while (iterator.hasNext()) {
                    Map.Entry<UK, UV> entry = iterator.next();
                    currentValue.put(entry.getKey(), entry.getValue());
                }
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
    public void setCurrentKey(K key) {
        currentKey = key;
    }

    @Override
    public RunnableWithException notifyLocalSnapshotStarted(long checkpointId) throws Exception {
        Preconditions.checkState(currentlyReferencingCheckpointID == NO_CHECKPOINT_ID);
        currentlyReferencingCheckpointID = checkpointId;
        LinkedHashMapLRUCache<K, Map<UK, UV>> snapshotCache = lruCache.clone();
        
        return () -> {
            keyedStateBackendForCache.applyToAllKeys(
                    namespace,
                    namespaceSerializer,
                    cacheStateDescriptor,
                    (key, state) -> state.clear()
            );
            for (Map.Entry<K, Map<UK, UV>> entry : snapshotCache.entrySet()) {
                if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                    keyedStateBackendForCache.setCurrentKey(entry.getKey());
                    stateForCache.putAll(entry.getValue());
                }
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
    public TypeSerializer<K> getKeySerializer() {
        return stateForCache.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return stateForCache.getNamespaceSerializer();
    }

    @Override
    public TypeSerializer<Map<UK, UV>> getValueSerializer() {
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
            TypeSerializer<Map<UK, UV>> safeValueSerializer) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public StateIncrementalVisitor<K, N, Map<UK, UV>> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
        throw new UnsupportedOperationException();
    }

    private void removalCallback(K keyToRemove, Map<UK, UV> value) {
        keyedStateBackend.setCurrentKey(keyToRemove);
        try {
            state.clear();
            if (value != null) {
                state.putAll(value);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static <K, N, UK, UV> MapStateWithCache<K, N, UK, UV> getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer,
            MapStateDescriptor<UK, UV> stateDescriptor,
            CheckpointableKeyedStateBackend<K> backend,
            CheckpointableKeyedStateBackend<K> backendForCache,
            int keySize) throws Exception {
        InternalMapState<K, N, UK, UV> state = 
            (InternalMapState<K, N, UK, UV>) backend.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
        InternalMapState<K, N, UK, UV> stateForCache =
            (InternalMapState<K, N, UK, UV>) backendForCache.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
        return new MapStateWithCache<>(
                (N) VoidNamespace.INSTANCE,
                namespaceSerializer,
                stateDescriptor,
                state,
                stateForCache,
                backend,
                backendForCache,
                keySize);
    }

    static <K, N, UK, UV> MapStateWithCache<K, N, UK, UV> getPartitionedState(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            MapStateDescriptor<UK, UV> stateDescriptor,
            CheckpointableKeyedStateBackend<K> backend,
            CheckpointableKeyedStateBackend<K> backendForCache,
            int keySize) throws Exception {
        InternalMapState<K, N, UK, UV> state =
            (InternalMapState<K, N, UK, UV>) backend.getPartitionedState(namespace, namespaceSerializer, stateDescriptor);
        InternalMapState<K, N, UK, UV> stateForCache =
            (InternalMapState<K, N, UK, UV>) backendForCache.getPartitionedState(namespace, namespaceSerializer, stateDescriptor);
        return new MapStateWithCache<>(
                namespace,
                namespaceSerializer,
                stateDescriptor,
                state,
                stateForCache,
                backend,
                backendForCache,
                keySize);
    }
}