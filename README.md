# flink-cassandra-keyspace-cluster
An extension for flink cassandra connector that lets you specify default cassandra keyspace.

<br />

The main adventages of the KeyspaceClusterBuilder is that it allows you to use POJO (using mappers http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/mapping/Mapper.html) without defining constant keyspace, and you can get keyspace from properties.

<br />

Example: 
```java
public class Main {
    public void main(String[] args) {
        String keyspace = "keyspace_flink";
        if(args.length > 0) {
            keyspace = args[0];
        }

        final TypeCodec<LocalDateTime> localDateTimeCodec = null; //Own codec

        //...
        CassandraPojoSink<Pojo> sink = new CassandraPojoSink<>(
            Pojo.class, 
            new KeyspaceClusterBuilder(keyspace) {
                @Override
                protected Cluster.Builder filledBuilder(Cluster.Builder builder) {
                    return builder.addContactPoint("localhost");
                }
                @Override
                protected void configureCluster(Cluster cluster) {
                    cluster
                        .getConfiguration()
                        .getCodecRegistry()
                        .register(localDateTimeCodec);
                }
            }
        );
        //...
    }
}

@Table(name = "test")
public class Pojo implements Serializable {
    //...
}
```

<br />

In standard solution you have to define constant keyspace in annotation:
```java
public class Main {
    public void main(String[] args) {
        //...

        final TypeCodec<LocalDateTime> localDateTimeCodec = null; //Own codec

        CassandraPojoSink<Pojo> sink = new CassandraPojoSink<>(
            Pojo.class, 
            new ClusterBuilder() {
                @Override
                protected Cluster buildCluster(Cluster.Builder builder) {
                    Cluster cluster = builder.addContactPoint("localhost").build();
                    cluster
                        .getConfiguration()
                        .getCodecRegistry()
                        .register(localDateTimeCodec);
                    return cluster;
                }
            }
        );
        //...
    }
}

@Table(keyspace = "keyspace_flink", name = "test")
public class Pojo implements Serializable {
    //...
}
```
