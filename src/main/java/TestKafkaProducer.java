import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.util.Collections.emptySet;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class TestKafkaProducer {

    private static final String TOPIC_NAME = "";
    private static MockProducer mockProducer;
    private static KafkaProducer kafkaProducer;

    @Test
    public void givenKeyValue_whenSend_thenVerifyHistory() throws ExecutionException, InterruptedException {

        MockProducer mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());

        KafkaProducer kafkaProducer = new KafkaProducer(mockProducer);
        Future<RecordMetadata> recordMetadataFuture = kafkaProducer.send("soccer",
                "{\"site\" : \"baeldung\"}");

        assertTrue(mockProducer.history().size() == 1);
        // assertTrue(mockProducer.history().get(0).key().equalsIgnoreCase("data"));
        assertTrue(recordMetadataFuture.get().partition() == 0);
    }

    @Test
    void givenKeyValue_whenSendWithPartitioning_thenVerifyPartitionNumber()
            throws ExecutionException, InterruptedException {
        PartitionInfo partitionInfo0 = new PartitionInfo(TOPIC_NAME, 0, null, null, null);
        PartitionInfo partitionInfo1 = new PartitionInfo(TOPIC_NAME, 1, null, null, null);
        List<PartitionInfo> list = new ArrayList<>();
        list.add(partitionInfo0);
        list.add(partitionInfo1);

        Cluster cluster = new Cluster("kafkab", new ArrayList<Node>(), list, emptySet(), emptySet());
        this.mockProducer = new MockProducer<>(cluster, true, new EvenOddPartitioner(),
                new StringSerializer(), new StringSerializer());

        KafkaProducer kafkaProducer = new KafkaProducer(mockProducer);
        Future<RecordMetadata> recordMetadataFuture = kafkaProducer.send("partition",
                "{\"site\" : \"baeldung\"}");

        assertTrue(recordMetadataFuture.get().partition() == 1);
    }

    @Test
    void givenKeyValue_whenSend_thenReturnException() {
        MockProducer<String, String> mockProducer = new MockProducer<>(false,
                new StringSerializer(), new StringSerializer());

        kafkaProducer = new KafkaProducer(mockProducer);
        Future<RecordMetadata> record = kafkaProducer.send("site", "{\"site\" : \"baeldung\"}");
        RuntimeException e = new RuntimeException();
        mockProducer.errorNext(e);

        try {
            record.get();
        } catch (ExecutionException | InterruptedException ex) {
            assertEquals(e, ex.getCause());
        }
        assertTrue(record.isDone());
    }

    @Test
    void givenKeyValue_whenSendWithTxn_thenSendOnlyOnTxnCommit() {
        MockProducer<String, String> mockProducer = new MockProducer<>(true,
                new StringSerializer(), new StringSerializer());

        kafkaProducer = new KafkaProducer(mockProducer);
       // kafkaProducer.initTransaction();
       // kafkaProducer.beginTransaction();
        Future<RecordMetadata> record = kafkaProducer.send("data", "{\"site\" : \"baeldung\"}");

        assertTrue(mockProducer.history().isEmpty());
       // kafkaProducer.commitTransaction();
        assertTrue(mockProducer.history().size() == 1);
    }

}
