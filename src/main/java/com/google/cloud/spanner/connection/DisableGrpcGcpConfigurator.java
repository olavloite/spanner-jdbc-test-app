package com.google.cloud.spanner.connection;

import com.google.cloud.spanner.SpannerOptions.Builder;
import com.google.cloud.spanner.connection.ConnectionOptions.SpannerOptionsConfigurator;

public class DisableGrpcGcpConfigurator implements SpannerOptionsConfigurator {

  @Override
  public void configure(Builder options) {
    options.disableGrpcGcpExtension();
  }
}
