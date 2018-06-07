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
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class KeyspaceClusterBuilderTest {

    @Test
    public void should_create_keyspace_cluster_with_filled_keyspace() {
        //given
        String keyspace = "KEYSPACE";
        KeyspaceClusterBuilder subject = new KeyspaceClusterBuilder(keyspace) {
            @Override
            protected Cluster.Builder filledBuilder(Cluster.Builder builder) {
                return builder
                        .addContactPoint("localhost");
            }
        };

        //when
        Cluster actual = subject.getCluster();

        //then
        assertThat(actual)
                .isExactlyInstanceOf(KeyspaceCluster.class)
                .hasFieldOrPropertyWithValue("keyspace", keyspace);
    }

    @Test
    public void should_create_keyspace_cluster_with_null_keyspace() {
        //given
        KeyspaceClusterBuilder subject = new KeyspaceClusterBuilder(null) {
            @Override
            protected Cluster.Builder filledBuilder(Cluster.Builder builder) {
                return builder
                        .addContactPoint("localhost");
            }
        };

        //when
        Cluster actual = subject.getCluster();

        //then
        assertThat(actual)
                .isExactlyInstanceOf(KeyspaceCluster.class)
                .hasFieldOrPropertyWithValue("keyspace", null);
    }
}