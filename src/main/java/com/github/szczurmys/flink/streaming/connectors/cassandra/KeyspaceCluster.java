/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.github.szczurmys.flink.streaming.connectors.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Cassandra {@link Cluster} with default keyspace when you call {@link KeyspaceCluster#connect()}
 */
class KeyspaceCluster extends Cluster {
    private final String keyspace;

    /**
     * Create cassandra {@link Cluster} with default keyspace when you call {@link KeyspaceCluster#connect()}
     *
     * @param keyspace    default keyspace for {@link KeyspaceCluster#connect()}
     * @param initializer Cassandra configuration,
     *                    mostly used {@link Cluster.Initializer} implementation is {@link Cluster.Builder}
     */
    KeyspaceCluster(String keyspace, Initializer initializer) {
        super(initializer);
        this.keyspace = keyspace;
    }

    public String getKeyspace() {
        return keyspace;
    }

    /**
     * Creates a new session on this cluster and sets
     * the default keyspace ({@link KeyspaceCluster#keyspace}) to the provided one.
     *
     * @return a new session on this cluster sets to default keyspace ({@link KeyspaceCluster#keyspace}).
     * @throws com.datastax.driver.core.exceptions.NoHostAvailableException if no host can be contacted
     *                                                                      to set the default keyspace ({@link KeyspaceCluster#keyspace}).
     */
    @Override
    public Session connect() {
        return connect(keyspace);
    }
}
