/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.util;

import org.apache.kafka.connect.sink.SinkRecord;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig.PrimaryKeyMode;

import static java.util.Objects.isNull;

import java.util.Iterator;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;

public class RecordUtil {
  public static boolean isRecordToDelete(
      SinkRecord record,
      PrimaryKeyMode pkMode
  ) {
    Struct value = (Struct) record.value();
    if (pkMode == PrimaryKeyMode.RECORD_KEY && isNull(value)) {
      return true;
    } else if (pkMode == PrimaryKeyMode.RECORD_VALUE) {
      Headers headers = record.headers();
      Iterator<Header> itHeaders = headers.allWithName("to_delete");
      boolean hasToBeDeleted = itHeaders.hasNext();

      while (itHeaders.hasNext()) {
        Header header = itHeaders.next();
        hasToBeDeleted = hasToBeDeleted
          && header.value().toString().toLowerCase() == "true";
      }

      return hasToBeDeleted;
    }
    return false;
  }
}
