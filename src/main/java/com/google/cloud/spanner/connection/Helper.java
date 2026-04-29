package com.google.cloud.spanner.connection;

import com.google.cloud.spanner.connection.ConnectionOptions.SpannerOptionsConfigurator;

public class Helper {

  public static ConnectionOptions.Builder setConfigurator(ConnectionOptions.Builder options, SpannerOptionsConfigurator configurator) {
    return options.setConfigurator(configurator);
  }

}
