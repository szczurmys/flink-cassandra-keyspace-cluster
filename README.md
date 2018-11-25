flink-cassandra-keyspace-cluster
================================

[![Apache License, Version 2.0, January 2004](https://img.shields.io/github/license/apache/maven.svg?label=License)](license)
[![Maven Central](https://img.shields.io/maven-central/v/com.github.szczurmys/flink-cassandra-keyspace-cluster.svg?label=Maven%20Central)](https://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22com.github.szczurmys%22%20AND%20a%3A%22flink-cassandra-keyspace-cluster%22)


An extension for flink cassandra connector that lets you specify default cassandra keyspace.

<br />

**For flink version >= 1.6 you should not use it, because it does not work.**<br/>
They added defaultKeyspace parameter for connector builder:
```java
        CassandraSink.addSink(dataSource)
                //...
                .setDefaultKeyspace("Your default keyspace")
                //...
                .build();
```



The main adventages (for flink version < 1.6) of the KeyspaceClusterBuilder is that it allows you to use POJO (using mappers http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/mapping/Mapper.html) without defining constant keyspace, and you can get keyspace from properties.

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
