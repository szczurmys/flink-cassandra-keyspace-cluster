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
    
        //...
        CassandraPojoSink<Pojo> sink = new CassandraPojoSink<>(
            Pojo.class, 
            new KeyspaceClusterBuilder(keyspace) {
                @Override
                protected Cluster.Builder filledBuilder(Cluster.Builder builder) {
                    return builder.addContactPoint("localhost");
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
        CassandraPojoSink<Pojo> sink = new CassandraPojoSink<>(
            Pojo.class, 
            new ClusterBuilder() {
                @Override
                protected Cluster buildCluster(Cluster.Builder builder) {
                    return builder.addContactPoint("localhost").build();
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
