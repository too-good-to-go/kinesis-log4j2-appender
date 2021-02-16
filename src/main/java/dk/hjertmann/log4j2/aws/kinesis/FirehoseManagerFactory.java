/*
 * Copyright (C) 2018 https://github.com/Minamoto54
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Modifications copyright (C) 2018 Robert Hjertmann Christiansen
 * 
 */
package dk.hjertmann.log4j2.aws.kinesis;

import java.io.IOException;
import java.io.Writer;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.StringLayout;
import org.apache.logging.log4j.core.appender.ManagerFactory;
import org.apache.logging.log4j.core.appender.WriterManager;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsync;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;
import dk.hjertmann.log4j2.aws.kinesis.FirehoseManagerFactory.InitParameters;

public class FirehoseManagerFactory implements ManagerFactory<WriterManager, InitParameters> {

  public static class FirehoseWriter extends Writer {

    private final InitParameters params;
    private final ByteBuffer batchBuffer;
    private final ScheduledExecutorService scheduledExecutor;
    private AmazonKinesisFirehoseAsync firehoseClient;
    private ScheduledFuture<?> awaitTask;

    public FirehoseWriter(final InitParameters params) {
      this.params = params;
      initFirehoseClient();
      batchBuffer = ByteBuffer.allocate(1024 * params.bufferSize);
      scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
      reScheduleTask();
    }

    @Override
    public void close() throws IOException {
      scheduledExecutor.shutdownNow();
      flushBatchBuffer(false);
      firehoseClient.shutdown();
    }

    @Override
    public void flush() throws IOException {}

    private synchronized void flushBatchBuffer() {
      flushBatchBuffer(true);
    }

    private synchronized void flushBatchBuffer(final boolean async) {
      if (batchBuffer.position() > 0) {
        final byte[] src = new byte[batchBuffer.position()];
        batchBuffer.rewind();
        batchBuffer.get(src).clear();
        putRecord(src, async);
      }
    }

    private void initFirehoseClient() {
      ClientConfiguration clientConfiguration = new ClientConfiguration();
      clientConfiguration = AwsUtil.setProxySettingsFromSystemProperties(clientConfiguration);
      clientConfiguration.setMaxErrorRetry(params.maxRetries);
      clientConfiguration
          .setRetryPolicy(new RetryPolicy(PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
              PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY, params.maxRetries, true));

      try {
        AmazonKinesisFirehoseAsyncClientBuilder configuration =
            AmazonKinesisFirehoseAsyncClientBuilder.standard()
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .withClientConfiguration(clientConfiguration);
        if (StringUtils.isBlank(params.endpoint)) {
          configuration.withRegion(params.region);
        } else {
          EndpointConfiguration endpointConfiguration =
              new EndpointConfiguration(params.endpoint, params.region);
          configuration.withEndpointConfiguration(endpointConfiguration);
        }
        firehoseClient = configuration
            .build();

        // Can't do this check because we get rate-limit error from AWS after release when all our
        // instances start up and do this check at the same time
        //
        // final DescribeDeliveryStreamResult describeResult =
        // firehoseClient.describeDeliveryStream(
        // new DescribeDeliveryStreamRequest().withDeliveryStreamName(params.deliveryStreamName));
        // final String streamStatus =
        // describeResult.getDeliveryStreamDescription().getDeliveryStreamStatus();
        // if (!DeliveryStreamStatus.ACTIVE.name().equals(streamStatus)) {
        // throw new IllegalStateException("DeliveryStream " + params.deliveryStreamName
        // + " is not ready (in active status) for appender: " + params.name);
        // }
      } catch (final Exception e) {
        throw new IllegalStateException("DeliveryStream " + params.deliveryStreamName
            + " doesn't exist for appender: " + params.name, e);
      }
    }

    private void putRecord(final byte[] src) {
      putRecord(src, true);
    }

    private void putRecord(final byte[] src, final boolean async) {
      try {
        if (async) {
          firehoseClient.putRecordAsync(
              new PutRecordRequest().withDeliveryStreamName(params.deliveryStreamName)
                  .withRecord(new Record().withData(ByteBuffer.wrap(src))));
        } else {
          firehoseClient
              .putRecord(new PutRecordRequest().withDeliveryStreamName(params.deliveryStreamName)
                  .withRecord(new Record().withData(ByteBuffer.wrap(src))));
        }
      } catch (final Exception ex) {
        LogManager.getLogger()
            .error("Failed to schedule log entry for publishing into firehose stream: "
                + params.deliveryStreamName);
      }
    }

    private void reScheduleTask() {
      if (awaitTask != null) {
        awaitTask.cancel(false);
      }
      awaitTask = scheduledExecutor.schedule(new Runnable() {
        @Override
        public void run() {
          flushBatchBuffer();
          reScheduleTask();
        }
      }, params.maxPutRecordDelay, TimeUnit.MINUTES);
    }

    private synchronized void transferData(final byte[] src) {
      try {
        batchBuffer.put(src);
      } catch (final BufferOverflowException e) {
        flushBatchBuffer();
        if (src.length > batchBuffer.capacity()) {
          putRecord(src);
        } else {
          batchBuffer.put(src);
        }
        reScheduleTask();
      }
    }

    @Override
    public void write(final char[] cbuf, final int off, final int len) throws IOException {
      final byte[] data = new String(cbuf, off, len).getBytes(params.charset);
      if (len > 1024000) {
        LogManager.getLogger()
            .error("A log its size larger than 1000 KB can't be sent to Firehose.\n"
                + new String(cbuf, off, len));
        return;
      }
      transferData(data);
    }
  }

  public static class InitParameters {
    String name;
    String deliveryStreamName;
    String endpoint = "";
    Charset charset = StandardCharsets.UTF_8;
    int maxRetries = 3;
    String region = Regions.AP_NORTHEAST_1.getName();
    int bufferSize = 1000; // KB, 5 ~ 1000.
    int maxPutRecordDelay = 5; // minute, 1 ~ 60.
    StringLayout layout;

    public InitParameters(final String name, final String deliveryStreamName, final String endpoint,
        final String encoding,
        final int maxRetries, final String region, final int bufferSize,
        final int maxPutRecordDelay,
        final StringLayout layout) {

      this.name = name;
      if (StringUtils.isBlank(deliveryStreamName)) {
        throw new IllegalArgumentException(
            "Invalid configuration - deliveryStreamName cannot be null for appender: " + name);
      }
      this.deliveryStreamName = deliveryStreamName.trim();

      if (StringUtils.isNotBlank(encoding)) {
        try {
          charset = Charset.forName(encoding.trim());
        } catch (final Exception e) {
        }
      }

      if (StringUtils.isNotBlank(endpoint)) {
        this.endpoint = endpoint.trim();
      }

      this.maxRetries = getDefaultIfZero(maxRetries, this.maxRetries);

      if (StringUtils.isBlank(region)) {
        throw new IllegalArgumentException(
            "Invalid configuration - region cannot be null for appender: " + name);
      }
      this.region = region.trim();

      this.bufferSize = Math.min(Math.max(5, getDefaultIfZero(bufferSize, this.bufferSize)), 1000);
      this.maxPutRecordDelay =
          Math.min(Math.max(1, getDefaultIfZero(maxPutRecordDelay, this.maxPutRecordDelay)), 60);
      this.layout = layout;
    }

    private int getDefaultIfZero(final int test, final int def) {
      return test == 0 ? def : test;
    }
  }

  @Override
  public WriterManager createManager(final String name, final InitParameters params) {
    return new WriterManager(new FirehoseWriter(params), params.deliveryStreamName, params.layout,
        false);
  }

}
