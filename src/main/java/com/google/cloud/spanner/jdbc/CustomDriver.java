package com.google.cloud.spanner.jdbc;

import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.SessionPoolOptionsHelper;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.connection.AggressiveDynamicScalingConfigurator;
import com.google.cloud.spanner.connection.ConnectionOptions;
import com.google.cloud.spanner.connection.ConnectionOptionsHelper;
import com.google.cloud.spanner.connection.DisableGrpcGcpConfigurator;
import com.google.cloud.spanner.connection.Helper;
import com.google.rpc.Code;
import io.opentelemetry.api.OpenTelemetry;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Properties;

public class CustomDriver extends JdbcDriver {
  static {
    try {
      register();
    } catch (SQLException e) {
      java.sql.DriverManager.println("Registering driver failed: " + e.getMessage());
    }
  }

  private static CustomDriver registeredDriver;

  static void register() throws SQLException {
    if (isRegistered()) {
      throw new IllegalStateException(
          "Driver is already registered. It can only be registered once.");
    }
    CustomDriver registeredDriver = new CustomDriver();
    DriverManager.registerDriver(registeredDriver);
    CustomDriver.registeredDriver = registeredDriver;
  }

  /**
   * According to JDBC specification, this driver is registered against {@link DriverManager} when
   * the class is loaded. To avoid leaks, this method allow unregistering the driver so that the
   * class can be gc'ed if necessary.
   *
   * @throws IllegalStateException if the driver is not registered
   * @throws SQLException if deregistering the driver fails
   */
  static void deregister() throws SQLException {
    if (!isRegistered()) {
      throw new IllegalStateException(
          "Driver is not registered (or it has not been registered using Driver.register() method)");
    }
    ConnectionOptions.closeSpanner();
    DriverManager.deregisterDriver(registeredDriver);
    registeredDriver = null;
  }

  /**
   * @return {@code true} if the driver is registered against {@link DriverManager}
   */
  static boolean isRegistered() {
    return registeredDriver != null;
  }

  /**
   * @return the registered JDBC driver for Cloud Spanner.
   * @throws SQLException if the driver has not been registered.
   */
  static JdbcDriver getRegisteredDriver() throws SQLException {
    if (isRegistered()) {
      return registeredDriver;
    }
    throw JdbcSqlExceptionFactory.of(
        "The driver has not been registered", Code.FAILED_PRECONDITION);
  }

  @Override
  public boolean acceptsURL(String url) {
    return url.startsWith("jdbc:custom_spanner_driver");
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (url != null && url.startsWith("jdbc:custom_spanner_driver")) {
      try {
        // strip 'jdbc:' from the URL, add any extra properties and pass on to the generic
        // Connection API. Also set the user-agent if we detect that the connection
        // comes from known framework like Hibernate, and there is no other user-agent set.
        maybeAddUserAgent(info);
        String connectionUri = appendPropertiesToUrl(url.replace("jdbc:custom_spanner_driver", "cloudspanner"), info);
        ConnectionOptions options = buildConnectionOptions(connectionUri, info);
        JdbcConnection connection = new JdbcConnection(url, options);
        if (options.getWarnings() != null) {
          connection.pushWarning(new SQLWarning(options.getWarnings()));
        }
        return connection;
      } catch (SpannerException e) {
        throw JdbcSqlExceptionFactory.of(e);
      } catch (IllegalArgumentException e) {
        throw JdbcSqlExceptionFactory.of(e.getMessage(), Code.INVALID_ARGUMENT, e);
      } catch (Exception e) {
        throw JdbcSqlExceptionFactory.of(e.getMessage(), Code.UNKNOWN, e);
      }
    }
    return null;
  }

  static ConnectionOptions buildConnectionOptions(String connectionUrl, Properties info) {
    ConnectionOptions.Builder builder =
        ConnectionOptions.newBuilder().setTracingPrefix("JDBC").setUri(connectionUrl);
    if (info.containsKey(OPEN_TELEMETRY_PROPERTY_KEY)
        && info.get(OPEN_TELEMETRY_PROPERTY_KEY) instanceof OpenTelemetry) {
      builder.setOpenTelemetry((OpenTelemetry) info.get(OPEN_TELEMETRY_PROPERTY_KEY));
    }
    // Enable multiplexed sessions by default for the JDBC driver.
    builder.setSessionPoolOptions(
        SessionPoolOptionsHelper.useMultiplexedSessions(SessionPoolOptions.newBuilder()).build());
    // Enable direct executor for JDBC, as we don't use the async API.
    builder =
        ConnectionOptionsHelper.useDirectExecutorIfNotUseVirtualThreads(connectionUrl, builder);
    boolean disableGrpcGcp = Boolean.parseBoolean(System.getenv().getOrDefault("DISABLE_GCP", "false"));
    boolean aggressiveScaling = Boolean.parseBoolean(System.getenv().getOrDefault("AGGRESSIVE_SCALING", "false"));

    if (disableGrpcGcp) {
      builder = Helper.setConfigurator(builder, new DisableGrpcGcpConfigurator());
    } else if (aggressiveScaling) {
      builder = Helper.setConfigurator(builder, new AggressiveDynamicScalingConfigurator());
    }
    return builder.build();
  }
}
