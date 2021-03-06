package com.atstec;

// https://github.com/yukim/cassandra-bulkload-example

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

/** Usage: java bulkload.com.atstec.BulkLoad */
public class BulkLoad {
  public static final String CSV_URL = "http://real-chart.finance.yahoo.com/table.csv?s=%s";

  /** Default output directory */
  public static String DEFAULT_OUTPUT_DIR = "./data";

  public static final SimpleDateFormat DATE_FORMAT =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
  public static final SimpleDateFormat DATE_FORMAT2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  /* 2021-02-06 05:31:24.088902+03*/
  // public static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd
  // HH:mm:ss.SSSSSSx");

  /** Keyspace name */
  public static final String KEYSPACE = "omniowner";
  /** Table name */
  public static final String TABLE = "order_status_change_t";

  /**
   * Schema for bulk loading table. It is important not to forget adding keyspace name before table
   * name, otherwise CQLSSTableWriter throws exception.
   */
  public static final String SCHEMA =
      String.format(
          "create table %s.%s ("
              + "                             id uuid,"
              + "                             change_date timestamp,"
              + "                             changer_id text,"
              + "                             execution_status text,"
              + "                             location_id uuid,"
              + "                             mile_type text,"
              + "                             order_id uuid,"
              + "                             sender_id uuid STATIC,"
              + "                             sender_order_id text STATIC,"
              + "                             status text,"
              + "                             status_change_reason_desc text,"
              + "                             PRIMARY KEY ((order_id), id))",
          KEYSPACE, TABLE);

  /**
   * INSERT statement to bulk load. It is like prepared statement. You fill in place holder for each
   * data.
   */
  public static final String INSERT_STMT =
      String.format(
          "INSERT INTO %s.%s ("
              + "id,order_id,status,change_date,changer_id,execution_status,status_change_reason_desc,location_id,mile_type,sender_order_id,sender_id"
              + ") VALUES ("
              + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
              + ")",
          KEYSPACE, TABLE);

  public static void main(String[] args) {
    long startTime = System.currentTimeMillis();
    String filename = null;

    if (args.length == 0) {
      System.out.println("usage: 1 param - source filename, 2 param - output directory");
      return;
    } else if (args.length == 1) {
      filename = args[0];
    } else if (args.length == 2) {
      filename = args[0];
      DEFAULT_OUTPUT_DIR = args[1];
    }

    // magic!
    Config.setClientMode(true);

    // Create output directory that has keyspace and table name in the path
    File outputDir =
        new File(DEFAULT_OUTPUT_DIR + File.separator + KEYSPACE + File.separator + TABLE);
    if (!outputDir.exists() && !outputDir.mkdirs()) {
      throw new RuntimeException("Cannot create output directory: " + outputDir);
    }

    // Prepare SSTable writer
    CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
    // set output directory
    builder
        .inDirectory(outputDir)
        // set target schema
        .forTable(SCHEMA)
        // set CQL statement to put data
        .using(INSERT_STMT)
        // set partitioner if needed
        // default is Murmur3Partitioner so set if you use different one.
        .withPartitioner(new Murmur3Partitioner());
    CQLSSTableWriter writer = builder.build();

    try (BufferedReader reader = Files.newBufferedReader(Paths.get(filename));
        CsvListReader csvReader = new CsvListReader(reader, CsvPreference.EXCEL_PREFERENCE)) {

      csvReader.getHeader(true);

      // Write to SSTable while reading data
      List<String> line;
      while ((line = csvReader.read()) != null) {
        // We use Java types here based on
        // http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/DataType.Name.html#asJavaClass%28%29
        writer.addRow(
            line.get(0) == null ? null : UUID.fromString(line.get(0)),
            line.get(1) == null ? null : UUID.fromString(line.get(1)),
            line.get(2) == null ? null : line.get(2),
            line.get(3) == null ? null : parseDate(line.get(3)),
            line.get(4) == null ? null : line.get(4),
            line.get(5) == null ? null : line.get(5),
            line.get(6) == null ? null : line.get(6),
            line.get(7) == null ? null : UUID.fromString(line.get(7)),
            line.get(8) == null ? null : line.get(8),
            line.get(9) == null ? null : line.get(9),
            line.get(10) == null ? null : UUID.fromString(line.get(10)));
      }
    } catch (InvalidRequestException | IOException | ParseException e) {
      e.printStackTrace();
    }

    try {
      writer.close();
    } catch (IOException ignore) {
    }

    long endTime = System.currentTimeMillis();
    System.out.println("\n\n\n That took " + ((double) (endTime - startTime)) / 60000 + " minutes");
  }

  private static Date parseDate(String date) throws ParseException {
    Date result = null;
    try {
      result = DATE_FORMAT.parse(date);
    } catch (ParseException e) {
      try {
        result = DATE_FORMAT2.parse(date);
      } catch (Exception x) {
        System.out.println(x + "  - set date time to now");
        return java.sql.Date.valueOf(LocalDate.now());
      }
    }
    return result;
  }
}
