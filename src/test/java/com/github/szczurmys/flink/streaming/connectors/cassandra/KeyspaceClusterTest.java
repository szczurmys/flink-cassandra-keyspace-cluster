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
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;


public class KeyspaceClusterTest {

    @Test
    public void create_session_with_keyspace_if_present() {
        //given
        String keyspace = "KEYSPACE";
        KeyspaceCluster subject = spy(new KeyspaceCluster(keyspace, createBuilder()));
        Session session = mock(Session.class);

        doReturn(session).when(subject).connect(keyspace);

        //when
        Session actual = subject.connect();

        //then
        assertThat(actual).isEqualTo(session);
        verify(subject, times(1)).connect(keyspace);
    }

    @Test
    public void create_session_with_keyspace_even_if_is_null() {
        //given
        KeyspaceCluster subject = spy(new KeyspaceCluster(null, createBuilder()));
        Session session = mock(Session.class);

        doReturn(session).when(subject).connect(null);

        //when
        Session actual = subject.connect();

        //then
        assertThat(actual).isEqualTo(session);
        verify(subject, times(1)).connect(null);
    }


    private Cluster.Builder createBuilder() {
        return new Cluster.Builder()
                .addContactPoint("localhost")
                ;
    }

}