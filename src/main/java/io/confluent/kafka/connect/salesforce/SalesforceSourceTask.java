/**
 * Copyright (C) 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.kafka.connect.salesforce;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import io.confluent.kafka.connect.salesforce.rest.SalesforceRestClient;
import io.confluent.kafka.connect.salesforce.rest.SalesforceRestClientFactory;
import io.confluent.kafka.connect.salesforce.rest.model.ApiVersion;
import io.confluent.kafka.connect.salesforce.rest.model.AuthenticationResponse;
import io.confluent.kafka.connect.salesforce.rest.model.SObjectDescriptor;
import io.confluent.kafka.connect.salesforce.rest.model.SObjectMetadata;
import io.confluent.kafka.connect.salesforce.rest.model.SObjectsResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.cometd.bayeux.Channel;
import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.bayeux.client.ClientSessionChannel.MessageListener;
import org.cometd.client.BayeuxClient;
import org.cometd.client.http.jetty.JettyHttpClientTransport;
import org.cometd.client.transport.ClientTransport;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SalesforceSourceTask
  extends SourceTask
  implements ClientSessionChannel.MessageListener {

  private static final Long REPLAY_FROM_EARLY = -2L;
  private static final Logger log = LoggerFactory.getLogger(SalesforceSourceTask.class);
  private final ConcurrentMap<String, Long> replay = new ConcurrentHashMap<>();
  final ConcurrentLinkedDeque<SourceRecord> messageQueue = new ConcurrentLinkedDeque<>();
  SalesforceSourceConfig config;
  SalesforceRestClient salesforceRestClient;
  AuthenticationResponse authenticationResponse;
  SObjectDescriptor descriptor;
  SObjectMetadata metadata;
  ApiVersion apiVersion;
  GenericUrl streamingUrl;
  BayeuxClient streamingClient;
  Schema keySchema;
  Schema valueSchema;
  ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  BayeuxClient createClient() {
    HttpClient httpClient = new HttpClient();
    httpClient.setConnectTimeout(this.config.connectTimeout());

    try {
      httpClient.start();
    } catch (Exception e) {
      throw new ConnectException("Exception thrown while starting httpClient.", e);
    }

    Map<String, Object> options = new HashMap<>();

    ClientTransport transport = new JettyHttpClientTransport(options, httpClient) {
      @Override
      protected void customize(Request request) {
        super.customize(request);
        String headerValue = String.format(
          "Authorization: %s %s",
          authenticationResponse.tokenType(),
          authenticationResponse.accessToken()
        );
        request.header("Authorization", headerValue);
      }
    };

    return new BayeuxClient(this.streamingUrl.toString(), transport);
  }

  private class AuthFailureListener implements ClientSessionChannel.MessageListener {
    private static final String ERROR_401 = "401";
    private static final String ERROR_403 = "403";

    @Override
    public void onMessage(ClientSessionChannel channel, Message message) {
      if (!message.isSuccessful()) {
        if (isError(message, ERROR_401) || isError(message, ERROR_403)) {
          log.info("Reconnecting... {}", message);
          disconnect();
          connect();
        }
      }
    }

    private boolean isError(Message message, String errorCode) {
      String error = (String)message.get(Message.ERROR_FIELD);
      String failureReason = getFailureReason(message);

      return (error != null && error.startsWith(errorCode)) ||
              (failureReason != null && failureReason.startsWith(errorCode));
    }

    private String getFailureReason(Message message) {
      String failureReason = null;
      Map<String, Object> ext = message.getExt();
      if (ext != null) {
        Map<String, Object> sfdc = (Map<String, Object>)ext.get("sfdc");
        if (sfdc != null) {
            failureReason = (String)sfdc.get("failureReason");
        }
      }
      return failureReason;
    }
  }

  @Override
  public void start(Map<String, String> map) {
    this.config = new SalesforceSourceConfig(map);
    this.salesforceRestClient = SalesforceRestClientFactory.create(this.config);
    this.authenticationResponse = this.salesforceRestClient.authenticate();

    List<ApiVersion> apiVersions = salesforceRestClient.apiVersions();

    for (ApiVersion v : apiVersions) {
      if (this.config.version().equals(v.version())) {
        apiVersion = v;
        break;
      }
    }

    Preconditions.checkNotNull(
      apiVersion,
      "Could not find ApiVersion '%s'",
      this.config.version()
    );
    salesforceRestClient.apiVersion(apiVersion);

    SObjectsResponse sObjectsResponse = salesforceRestClient.objects();

    for (SObjectMetadata metadata : sObjectsResponse.sobjects()) {
      if (this.config.salesForceObject().equals(metadata.name())) {
        this.descriptor = salesforceRestClient.describe(metadata);
        this.metadata = metadata;
        break;
      }
    }

    Preconditions.checkNotNull(
      this.descriptor,
      "Could not find descriptor for '%s'",
      this.config.salesForceObject()
    );

    this.keySchema = SObjectHelper.keySchema(this.descriptor);
    this.valueSchema =
      SObjectHelper.valueSchema(
        this.descriptor,
        this.config.salesForcePushTopicFields()
      );

    this.streamingUrl = new GenericUrl(this.authenticationResponse.instance_url());
    this.streamingUrl.setRawPath(
        String.format("/cometd/%s", this.apiVersion.version())
      );

    replay.clear();
    this.connect();
  }

  public void disconnect() {
    this.streamingClient.disconnect();
    this.streamingClient.waitFor(1000, BayeuxClient.State.DISCONNECTED);
  }

  public String getChannel() {
    return String.format("/topic/%s", this.config.salesForcePushTopicName());
  }

  public void setReplayId(String channel) {
    if (!replay.isEmpty()) {
      log.info("Replay already setted: {}", replay);
      return;
    }

    Map<String, String> sourcePartition = Collections
      .singletonMap("pushTopicName", this.config.salesForcePushTopicName());
    Map<String, Object> offset = context.offsetStorageReader().offset(sourcePartition);

    if (offset != null) {
      Long lastRecordedReplayId = (Long) offset.get("replayId");
      Long lastTimestamp = (Long) offset.get("timestamp");
      Long nowTimestamp = System.currentTimeMillis();

      if (lastRecordedReplayId != null && lastTimestamp != null && lastTimestamp > (nowTimestamp - 24 * 3600 * 1000)) {
        log.info("Replay Id recovered: {} {}", lastRecordedReplayId, offset);
        replay.putIfAbsent(channel, lastRecordedReplayId);
        return;
      }
    }

    log.warn("Replaying from early (24h ago)");
    replay.putIfAbsent(channel, REPLAY_FROM_EARLY);
  }

  public void connect() {
    log.info("Configuring streaming url to {}", this.streamingUrl);
    this.setReplayId(this.getChannel());
    this.streamingClient = createClient();
    this.streamingClient.addExtension(new ReplayExtension(replay));
    this.streamingClient.getChannel(Channel.META_CONNECT)
      .addListener(new AuthFailureListener());
    this.streamingClient.getChannel(Channel.META_HANDSHAKE)
      .addListener(new AuthFailureListener());
    this.streamingClient.handshake(
      handshakeReply -> {
        if (!handshakeReply.isSuccessful()) {
          log.error("Failed to make handshake with {} url", this.streamingUrl);

          return;
        }

        this.streamingClient.getChannel(this.getChannel())
          .subscribe(
            this,
            subscribeReply -> {
              if (subscribeReply.isSuccessful()) {
                log.info("Subscribe successful to {}", this.getChannel());
              }
            }
          );
      }
    );
  }

  @Override
  public List<SourceRecord> poll() {
    List<SourceRecord> records = new ArrayList<>(256);

    while (records.isEmpty()) {
      int size = messageQueue.size();

      for (int i = 0; i < size; i++) {
        SourceRecord record = this.messageQueue.poll();

        if (null == record) {
          break;
        }

        records.add(record);
      }

      if (records.isEmpty()) {
        return null;
      }
    }

    return records;
  }

  @Override
  public void stop() {
    disconnect();
  }

  @Override
  public void commitRecord(SourceRecord record,
                           org.apache.kafka.clients.producer.RecordMetadata metadata) throws InterruptedException {
    Map<String, ?> offset = record.sourceOffset();
    Long currentReplayId = (Long) offset.get("replayId");
    log.info("commitRecord: replayId {} with offset {}", currentReplayId, metadata.offset());
    replay.put(this.getChannel(), currentReplayId);
  }

  @Override
  public void onMessage(ClientSessionChannel clientSessionChannel, Message message) {
    try {
      Object messageObject = message.getData();

      log.info("message={}", messageObject);

      JsonNode jsonNode = objectMapper.valueToTree(messageObject);
      SourceRecord record = SObjectHelper.convert(
        jsonNode,
        this.config.salesForcePushTopicName(),
        this.config.kafkaTopic(),
        keySchema,
        valueSchema
      );
      this.messageQueue.add(record);
    } catch (Exception ex) {
      log.error("Exception thrown while processing message.", ex);
    }
  }
}
