package com.google.cloud.jdbcapp;

import com.google.cloud.opentelemetry.metric.GoogleCloudMetricExporter;
import com.google.cloud.opentelemetry.metric.MetricConfiguration;
import io.grpc.opentelemetry.GrpcOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class App {

  public static void main(String[] args) throws Exception {
    // Create a standard OpenTelemetry SDK.
    OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
        .setMeterProvider(SdkMeterProvider.builder()
            .registerMetricReader(PeriodicMetricReader.create(
                GoogleCloudMetricExporter.createWithConfiguration(MetricConfiguration.builder()
                        .setProjectId("appdev-soda-spanner-staging")
                    .build())))
            .build())
        .buildAndRegisterGlobal();
    // Create a gRPC OpenTelemetry SDK that uses the standard OpenTelemetry SDK.
    GrpcOpenTelemetry.newBuilder().sdk(openTelemetry).build().registerGlobal();

    try (Connection connection = DriverManager.getConnection("jdbc:cloudspanner:/projects/appdev-soda-spanner-staging/instances/knut-test-ycsb/databases/knut-test-db")) {
      for (int n=0; n<100; n++) {
        try (ResultSet resultSet = connection.createStatement()
            .executeQuery("SELECT 'Hello World'")) {
          while (resultSet.next()) {
            System.out.println("Greeting " + n + ": " + resultSet.getString(1));
          }
        }
        Thread.sleep(1000L);
      }
    }
  }

}
