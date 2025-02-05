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
import com.google.api.client.util.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.connect.salesforce.rest.model.SObjectDescriptor;
import io.confluent.kafka.connect.utils.data.Parser;
import io.confluent.kafka.connect.utils.data.type.DateTypeParser;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.Collections;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SObjectHelper {

  private static final Parser PARSER;

  static {
    Parser p = new Parser();
    p.registerTypeParser(
      Timestamp.SCHEMA,
      new DateTypeParser(
        TimeZone.getTimeZone("UTC"),
        new SimpleDateFormat("YYYY-MM-dd'T'HH:mm:ss.SSS'Z'")
      )
    );
    PARSER = p;
  }

  public static boolean isTextArea(SObjectDescriptor.Field field) {
    return "textarea".equalsIgnoreCase(field.type());
  }

  public static Schema schema(SObjectDescriptor.Field field) {
    SchemaBuilder builder = null;

    boolean optional = true;

    switch (field.type()) {
      case "id":
        optional = false;
        builder = SchemaBuilder.string().doc("Unique identifier for the object.");
        break;
      case "boolean":
        builder = SchemaBuilder.bool();
        break;
      case "date":
        builder = SchemaBuilder.string();
        break;
      case "address":
        builder = SchemaBuilder.string();
        break;
      case "string":
        builder = SchemaBuilder.string();
        break;
      case "double":
        builder = SchemaBuilder.float64();
        break;
      case "picklist":
        builder = SchemaBuilder.string();
        break;
      case "textarea":
        builder = SchemaBuilder.string();
        break;
      case "url":
        builder = SchemaBuilder.string();
        break;
      case "int":
        builder = SchemaBuilder.int32();
        break;
      case "reference":
        builder = SchemaBuilder.string();
        break;
      case "datetime":
        builder = SchemaBuilder.string();
        break;
      case "phone":
        builder = SchemaBuilder.string();
        break;
      case "currency":
        builder = SchemaBuilder.float64();
        break;
      case "email":
        builder = SchemaBuilder.string();
        break;
      case "decimal":
        builder = Decimal.builder(field.scale());
        break;
      default:
        builder = SchemaBuilder.string();
    }

    if (optional) {
      builder = builder.optional();
    }

    return builder.build();
  }

  public static Schema valueSchema(SObjectDescriptor descriptor) {
    String name = String.format(
      "%s.%s",
      SObjectHelper.class.getPackage().getName(),
      descriptor.name()
    );
    SchemaBuilder builder = SchemaBuilder.struct();
    builder.name(name);

    for (SObjectDescriptor.Field field : descriptor.fields()) {
      if (isTextArea(field)) {
        continue;
      }
      Schema schema = schema(field);
      builder.field(field.name(), schema);
    }

    return builder.build();
  }

  public static Schema valueSchema(SObjectDescriptor descriptor, String customFields) {
    String name = String.format(
      "%s.%s",
      SObjectHelper.class.getPackage().getName(),
      descriptor.name()
    );
    SchemaBuilder builder = SchemaBuilder.struct();
    builder.name(name);

    for (SObjectDescriptor.Field field : descriptor.fields()) {
      if (
        !(customFields == null || customFields.isEmpty()) &&
        !customFields.contains(field.name())
      ) {
        continue;
      }
      if (isTextArea(field)) {
        continue;
      }
      Schema schema = schema(field);
      builder.field(field.name(), schema);
    }

    return builder.build();
  }

  public static Schema keySchema(SObjectDescriptor descriptor) {
    String name = String.format(
      "%s.%sKey",
      SObjectHelper.class.getPackage().getName(),
      descriptor.name()
    );
    SchemaBuilder builder = SchemaBuilder.struct();
    builder.name(name);

    SObjectDescriptor.Field keyField = null;

    for (SObjectDescriptor.Field field : descriptor.fields()) {
      if ("id".equalsIgnoreCase(field.type())) {
        keyField = field;
        break;
      }
    }

    if (null == keyField) {
      throw new IllegalStateException(
        "Could not find an id field for " + descriptor.name()
      );
    }

    Schema keySchema = schema(keyField);
    builder.field(keyField.name(), keySchema);
    return builder.build();
  }

  public static void convertStruct(JsonNode data, Schema schema, Struct struct) {
    for (Field field : schema.fields()) {
      String fieldName = field.name();
      JsonNode valueNode = data.findValue(fieldName);
      Object value = PARSER.parseJsonNode(field.schema(), valueNode);
      struct.put(field, value);
    }
  }

  public static SourceRecord convert(
    JsonNode jsonNode,
    String pushTopicName,
    String topic,
    Schema keySchema,
    Schema valueSchema
  ) {
    Preconditions.checkNotNull(jsonNode);
    Preconditions.checkState(jsonNode.isObject());
    JsonNode eventNode = jsonNode.get("event");
    JsonNode sobjectNode = jsonNode.get("sobject");
    long replayId = eventNode.get("replayId").asLong();
    Struct keyStruct = new Struct(keySchema);
    Struct valueStruct = new Struct(valueSchema);
    convertStruct(sobjectNode, keySchema, keyStruct);
    convertStruct(sobjectNode, valueSchema, valueStruct);
    Map sourcePartition = Collections.singletonMap("pushTopicName", pushTopicName);
    Map<String, Long> sourceOffset = new HashMap<String, Long>();
    sourceOffset.put("replayId", replayId);
    sourceOffset.put("timestamp", System.currentTimeMillis());
    return new SourceRecord(
      sourcePartition,
      sourceOffset,
      topic,
      keySchema,
      keyStruct,
      valueSchema,
      valueStruct
    );
  }
}
