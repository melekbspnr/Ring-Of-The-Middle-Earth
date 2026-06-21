package rotr.streams;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public final class App {
    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    private App() {
    }

    public static void main(String[] args) throws Exception {
        AppConfig appConfig = AppConfig.fromEnv();
        GameConfig gameConfig = GameConfig.load(appConfig.configDir);
        Schema validatedSchema = new Schema.Parser().parse(Files.readString(appConfig.schemaDir.resolve("OrderValidated.avsc")));
        Schema dlqSchema = new Schema.Parser().parse(Files.readString(appConfig.schemaDir.resolve("DLQEntry.avsc")));

        KafkaStreams topology1 = new KafkaStreams(
            ValidationTopology.build(appConfig, gameConfig, validatedSchema, dlqSchema),
            streamProps(appConfig, "rotr-topology1")
        );
        KafkaStreams topology2 = new KafkaStreams(
            RouteRiskTopology.build(appConfig, gameConfig, validatedSchema),
            streamProps(appConfig, "rotr-topology2")
        );

        topology1.setUncaughtExceptionHandler(exception -> {
            LOG.error("Topology 1 crashed", exception);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });
        topology2.setUncaughtExceptionHandler(exception -> {
            LOG.error("Topology 2 crashed", exception);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            topology2.close(Duration.ofSeconds(10));
            topology1.close(Duration.ofSeconds(10));
            latch.countDown();
        }));

        topology1.start();
        topology2.start();
        LOG.info("Kafka Streams topologies started");
        latch.await();
    }

    static Properties streamProps(AppConfig appConfig, String appId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.brokers);
        props.put("schema.registry.url", appConfig.schemaRegistryUrl);
        props.put("auto.register.schemas", false);
        props.put("use.latest.version", true);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/" + appId);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        return props;
    }

    static Serde<GenericRecord> avroSerde(String schemaRegistryUrl) {
        GenericAvroSerde serde = new GenericAvroSerde();
        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", schemaRegistryUrl);
        config.put("auto.register.schemas", "false");
        config.put("use.latest.version", "true");
        serde.configure(config, false);
        return serde;
    }
}
