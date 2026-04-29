package com.google.cloud.jdbcapp;

import com.google.cloud.opentelemetry.metric.GoogleCloudMetricExporter;
import com.google.cloud.opentelemetry.metric.MetricConfiguration;
import com.google.cloud.spanner.jdbc.CustomDriver;
import io.grpc.opentelemetry.GrpcOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.metrics.Aggregation;
import java.util.List;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributeKey;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class App {

  private static final String DEFAULT_URL = "jdbc:cloudspanner:/projects/appdev-soda-spanner-staging/instances/knut-test-ycsb/databases/spring-data-jpa;numChannels=16";
  //private static final String DEFAULT_URL = "jdbc:custom_spanner_driver:/projects/appdev-soda-spanner-staging/instances/knut-test-ycsb/databases/spring-data-jpa;numChannels=16";
  //private static final String DEFAULT_URL = "jdbc:custom_spanner_driver:/projects/appdev-soda-spanner-staging/instances/knut-test-ycsb/databases/spring-data-jpa";
  private static LongHistogram latencyHistogram;

  public static void main(String[] args) throws Exception {
    Class.forName(CustomDriver.class.getName());

    // Create a standard OpenTelemetry SDK.
    OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
        .setMeterProvider(SdkMeterProvider.builder()
            .registerMetricReader(PeriodicMetricReader.create(
                GoogleCloudMetricExporter.createWithConfiguration(MetricConfiguration.builder()
                        .setProjectId("appdev-soda-spanner-staging")
                    .build())))
            .registerView(
                InstrumentSelector.builder()
                    .setName("test_method_latency")
                    .build(),
                View.builder()
                    .setAggregation(
                        Aggregation.explicitBucketHistogram(
                            List.of(1000.0, 2500.0, 5000.0, 7500.0, 10000.0, 15000.0, 20000.0, 25000.0, 30000.0, 40000.0, 50000.0, 75000.0, 100000.0,
                                150000.0, 200000.0)))
                    .build())
            .build())
        .buildAndRegisterGlobal();
    // Create a gRPC OpenTelemetry SDK that uses the standard OpenTelemetry SDK.
    GrpcOpenTelemetry.newBuilder().sdk(openTelemetry).build().registerGlobal();

    Meter meter = openTelemetry.getMeter("com.google.cloud.jdbcapp");
    latencyHistogram = meter.histogramBuilder("test_method_latency")
        .ofLongs()
        .setDescription("Latency of test methods")
        .setUnit("us")
        .build();

    String jdbcUrl = System.getenv().getOrDefault("JDBC_URL", DEFAULT_URL);
    int poolSize = Integer.parseInt(System.getenv().getOrDefault("POOL_SIZE", "1000"));
    int rateM1 = Integer.parseInt(System.getenv().getOrDefault("RATE_M1", "6000"));
    int rateM2 = Integer.parseInt(System.getenv().getOrDefault("RATE_M2", "8000"));
    int threads = Integer.parseInt(System.getenv().getOrDefault("THREADS", "1000"));
    boolean useFixedRate = Boolean.parseBoolean(System.getenv().getOrDefault("USE_FIXED_RATE", "false"));

    System.out.println("Starting benchmark with:");
    System.out.println("JDBC URL: " + jdbcUrl);
    System.out.println("Pool Size: " + poolSize);
    System.out.println("Rate M1: " + rateM1 + "/s");
    System.out.println("Rate M2: " + rateM2 + "/s");
    System.out.println("Threads: " + threads);
    System.out.println("Use Fixed Rate: " + useFixedRate);

    BlockingQueue<Connection> connectionPool = new ArrayBlockingQueue<>(poolSize);
    for (int i = 0; i < poolSize; i++) {
      connectionPool.add(DriverManager.getConnection(jdbcUrl));
    }

    ExecutorService executor = Executors.newFixedThreadPool(threads);

    if (useFixedRate) {
      ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
      if (rateM1 > 0) {
        scheduler.scheduleAtFixedRate(() -> executor.submit(() -> executeTestMethod(connectionPool, "select", App::testMethod1)), 0, 1_000_000L / rateM1, TimeUnit.MICROSECONDS);
      }
      if (rateM2 > 0) {
        scheduler.scheduleAtFixedRate(() -> executor.submit(() -> executeTestMethod(connectionPool, "insert", App::testMethod2)), 0, 1_000_000L / rateM2, TimeUnit.MICROSECONDS);
      }
    } else {
      // Schedule Test Method 1
      if (rateM1 > 0) {
        new Thread(() -> {
          while (!Thread.currentThread().isInterrupted()) {
            executor.submit(() -> executeTestMethod(connectionPool, "select", App::testMethod1));
            LockSupport.parkNanos(calculatePoissonDelay(rateM1));
          }
        }, "RateM1-Generator").start();
      }

      // Schedule Test Method 2
      if (rateM2 > 0) {
        new Thread(() -> {
          while (!Thread.currentThread().isInterrupted()) {
            executor.submit(() -> executeTestMethod(connectionPool, "insert", App::testMethod2));
            LockSupport.parkNanos(calculatePoissonDelay(rateM2));
          }
        }, "RateM2-Generator").start();
      }
    }

    // Keep running
    Thread.sleep(Long.MAX_VALUE);
  }

  @FunctionalInterface
  interface TestMethod {
    void run(Connection conn) throws SQLException;
  }

  private static long calculatePoissonDelay(double rate) {
    double u = ThreadLocalRandom.current().nextDouble();
    return (long) (-Math.log(1.0 - u) * 1_000_000_000L / rate);
  }

  private static void executeTestMethod(BlockingQueue<Connection> connectionPool, String methodName,
      TestMethod testMethod) {
    long startTime = System.nanoTime();
    Connection conn = null;
    try {
      conn = connectionPool.take();
      testMethod.run(conn);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (conn != null) {
        connectionPool.add(conn);
      }
    }
    long latencyUs = (System.nanoTime() - startTime) / 1000;
    latencyHistogram.record(latencyUs, Attributes.of(AttributeKey.stringKey("method"), methodName));
  }

  private static void testMethod1(Connection conn) throws SQLException {
    conn.setAutoCommit(true);
    long randomId = ThreadLocalRandom.current().nextLong(1, 100_000_001);

    try (PreparedStatement stmt = conn.prepareStatement("SELECT id, value FROM test WHERE id = ?")) {
      stmt.setLong(1, randomId);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          rs.getString("value");
        }
      }
    }
  }

  private static void testMethod2(Connection conn) throws SQLException {
    conn.setAutoCommit(false);
    try {
      long randomId = ThreadLocalRandom.current().nextLong(1, 100_000_001);
      // 1. Select
      boolean exists = false;
      try (PreparedStatement stmt = conn.prepareStatement("SELECT id FROM test WHERE id = ?")) {
        stmt.setLong(1, randomId);
        try (ResultSet rs = stmt.executeQuery()) {
          if (rs.next()) {
            exists = true;
          }
        }
      }

      // 2. Insert or Update
      String value = generateRandomString(50 + ThreadLocalRandom.current().nextInt(51)); // 50 to 100 chars
      if (exists) {
        try (PreparedStatement stmt = conn.prepareStatement("UPDATE test SET value = ? WHERE id = ?")) {
          stmt.setString(1, value);
          stmt.setLong(2, randomId);
          stmt.executeUpdate();
        }
      } else {
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test (id, value) VALUES (?, ?)")) {
          stmt.setLong(1, randomId);
          stmt.setString(2, value);
          stmt.executeUpdate();
        }
      }
      conn.commit();
    } catch (SQLException e) {
      conn.rollback();
      throw e;
    }
  }

  private static String generateRandomString(int length) {
    String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      sb.append(chars.charAt(ThreadLocalRandom.current().nextInt(chars.length())));
    }
    return sb.toString();
  }

}
