package com.google.cloud.spanner.connection;

import com.google.cloud.grpc.GcpManagedChannelOptions.GcpChannelPoolOptions;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.SpannerOptions.Builder;
import com.google.cloud.spanner.connection.ConnectionOptions.SpannerOptionsConfigurator;

public class AggressiveDynamicScalingConfigurator implements SpannerOptionsConfigurator {

  @Override
  public void configure(Builder options) {
    GcpChannelPoolOptions poolOptions =
        GcpChannelPoolOptions.newBuilder()
            .setMinSize(SpannerOptions.DEFAULT_DYNAMIC_POOL_MIN_CHANNELS)
            .setMaxSize(SpannerOptions.DEFAULT_DYNAMIC_POOL_MAX_CHANNELS)
            .setInitSize(SpannerOptions.DEFAULT_DYNAMIC_POOL_INITIAL_SIZE)
            .setDynamicScaling(
                1,
                10,
                SpannerOptions.DEFAULT_DYNAMIC_POOL_SCALE_DOWN_INTERVAL)
            .setAffinityKeyLifetime(SpannerOptions.DEFAULT_DYNAMIC_POOL_AFFINITY_KEY_LIFETIME)
            .setCleanupInterval(SpannerOptions.DEFAULT_DYNAMIC_POOL_CLEANUP_INTERVAL)
            .build();
    options.setGcpChannelPoolOptions(poolOptions);
  }
}
