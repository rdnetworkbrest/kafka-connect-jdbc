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

package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcDbWriter {
  private static final Logger log = LoggerFactory.getLogger(JdbcDbWriter.class);

  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  final CachedConnectionProvider cachedConnectionProvider;

  private boolean enableJdbcSchema = true ;


  JdbcDbWriter(final JdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure) {
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;

    this.cachedConnectionProvider = connectionProvider(
        config.connectionAttempts,
        config.connectionBackoffMs
    );
    enableJdbcSchema =  "false".equalsIgnoreCase(System.getenv("ENABLE_JDBC_SCHEMA"))
            ? false : true ;
  }

  protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
    return new CachedConnectionProvider(this.dbDialect, maxConnAttempts, retryBackoff) {
      @Override
      protected void onConnect(final Connection connection) throws SQLException {
        log.info("JdbcDbWriter Connected");
        connection.setAutoCommit(false);
      }
    };
  }

  void write(final Collection<SinkRecord> records)
      throws SQLException, TableAlterOrCreateException {
    final Connection connection = cachedConnectionProvider.getConnection();
    try {
      final Map<TableId, BufferedRecords> bufferByTable = new HashMap<>();
      for (SinkRecord record : records) {
        final TableId tableId = destinationTable(record.topic(), record);
        BufferedRecords buffer = bufferByTable.get(tableId);
        if (buffer == null) {
          buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
          bufferByTable.put(tableId, buffer);
        }
        buffer.add(record);
      }
      for (Map.Entry<TableId, BufferedRecords> entry : bufferByTable.entrySet()) {
        TableId tableId = entry.getKey();
        BufferedRecords buffer = entry.getValue();
        log.debug("Flushing records in JDBC Writer for table ID: {}", tableId);
        buffer.flush();
        buffer.close();
      }
      connection.commit();
    } catch (SQLException | TableAlterOrCreateException e) {
      try {
        connection.rollback();
      } catch (SQLException sqle) {
        e.addSuppressed(sqle);
      } finally {
        throw e;
      }
    }
  }

  void closeQuietly() {
    cachedConnectionProvider.close();
  }

  //String patternRecordExpr = ".*(\\$\\{record\\.(value|key)\\.(.*)\\}).*";
  //Pattern patternRecord = Pattern.compile(patternRecordExpr);

  String findSchemaValue(Struct value, List<String> lst) {
    for (String fieldName : lst) {
      Field field = value.schema().field(fieldName)  ;
      if (field != null) {
        String result = (String) value.get(field) ;
        return result ;
      }
    }
    return null ;
    //throw new DataException(lst.toString() + " are not a valid field name");
  }

  TableId destinationTable(String topic, SinkRecord record) {
    //    final String tableName = config.tableNameFormat.replace("${topic}", topic);
    String tableName = config.tableNameFormat.replace("${topic}", topic);
    /*
    Matcher matcher = patternRecord.matcher(tableName);
    if (matcher.find()) {
      //String all =  matcher.group(0) ;
      String allExpr =  matcher.group(1) ;
      String typeExpr =  matcher.group(2) ;
      String selectExpr =  matcher.group(3) ;
      switch (typeExpr) {
        case "value" :
          Object value = record.value();
          if (value != null && value instanceof Struct) {
            String schema = (String) ((Struct) value).get(selectExpr);
            if (schema != null) {
              tableName = tableName.replace(allExpr,schema) ;
            }
          }
          break ;
        default :
      }
    }
     */
    if (enableJdbcSchema) {
      List<String> schemaRecordValueFields = config.schemaRecordValueFields;
      Object value = record.value();
      if (value != null && value instanceof Struct && !schemaRecordValueFields.isEmpty()) {
        String schema = findSchemaValue((Struct) value, schemaRecordValueFields);
        if (schema != null) {
          tableName = "\"" + schema + "\"." + tableName;
        }
      }
    }
    /*
    if (schemaRecordValue != null && !"None".equals(schemaRecordValue)) {
      Object value = record.value();
      if (value != null && value instanceof Struct) {
        String schema = (String) ((Struct) value).get(schemaRecordValue);
        if (schema != null) {
          tableName = "\"" + schema + "\"." + tableName;
        }
      }
    }
    */
    if (tableName.isEmpty()) {
      throw new ConnectException(String.format(
              "Destination table name for topic '%s' is empty using the format string '%s'",
              topic,
              config.tableNameFormat
      ));
    }
    return dbDialect.parseTableIdentifier(tableName);
  }

}
