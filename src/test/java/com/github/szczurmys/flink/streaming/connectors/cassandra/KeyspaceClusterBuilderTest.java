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
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import org.junit.Test;

import java.nio.ByteBuffer;

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
    public void should_create_configured_keyspace_cluster_with_filled_keyspace() {
        //given
        final ExampleCodec codec = new ExampleCodec();
        String keyspace = "KEYSPACE";
        KeyspaceClusterBuilder subject = new KeyspaceClusterBuilder(keyspace) {
            @Override
            protected Cluster.Builder filledBuilder(Cluster.Builder builder) {
                return builder
                        .addContactPoint("localhost");
            }

            @Override
            protected void configureCluster(Cluster cluster) {
                cluster
                        .getConfiguration()
                        .getCodecRegistry()
                        .register(codec);
            }
        };

        //when
        Cluster actual = subject.getCluster();
        TypeCodec<?> actualCodec = actual
                .getConfiguration()
                .getCodecRegistry()
                .codecFor(DataType.blob(), KeyspaceCluster.class);

        //then
        assertThat(actual)
                .isExactlyInstanceOf(KeyspaceCluster.class)
                .hasFieldOrPropertyWithValue("keyspace", keyspace);
        assertThat(actualCodec).isEqualTo(codec);
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

    private static class ExampleCodec extends TypeCodec<KeyspaceCluster> {

        ExampleCodec() {
            super(DataType.blob(), KeyspaceCluster.class);
        }

        public ByteBuffer serialize(KeyspaceCluster keyspaceCluster, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return null;
        }

        public KeyspaceCluster deserialize(ByteBuffer byteBuffer, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return null;
        }

        public KeyspaceCluster parse(String s) throws InvalidTypeException {
            return null;
        }

        public String format(KeyspaceCluster keyspaceCluster) throws InvalidTypeException {
            return null;
        }
    }
}