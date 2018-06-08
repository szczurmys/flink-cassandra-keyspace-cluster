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
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

/**
 * This class is used to configure a {@link com.datastax.driver.core.Cluster} with default keyspace after deployment.
 * The cluster represents the connection that will be established to Cassandra.
 */
public abstract class KeyspaceClusterBuilder extends ClusterBuilder {
    private static final long serialVersionUID = -5026923337624734541L;

    private String keyspace;

    public KeyspaceClusterBuilder(String keyspace) {
        this.keyspace = keyspace;
    }

    @Override
    protected final Cluster buildCluster(Cluster.Builder builder) {
        KeyspaceCluster keyspaceCluster = new KeyspaceCluster(keyspace, filledBuilder(builder));
        configureCluster(keyspaceCluster);
        return keyspaceCluster;
    }

    /**
     * Configures the connection builder to Cassandra.
     * The configuration is done by calling methods on the builder object
     * and finalizing the configuration with new KeyspaceCluster(...).
     *
     * @param builder connection builder
     * @return configured connection builder
     */
    protected abstract Cluster.Builder filledBuilder(Cluster.Builder builder);

    /**
     * Configures built cluster.
     * The configuration is done by calling methods on the cluster object
     * and finalizing the configuration with connect(...).
     *
     * @param cluster built cluster
     */
    protected void configureCluster(Cluster cluster) {
    }
}
