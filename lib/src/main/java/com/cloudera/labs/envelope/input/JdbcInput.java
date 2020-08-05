/*
 * Copyright (c) 2015-2020, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.labs.envelope.input;

import com.cloudera.labs.envelope.component.ProvidesAlias;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Properties;

public class JdbcInput implements BatchInput, ProvidesAlias, ProvidesValidations {

  public static final String JDBC_CONFIG_URL = "url";
  public static final String JDBC_CONFIG_TABLENAME = "tablename";
  public static final String JDBC_CONFIG_USERNAME = "username";
  public static final String JDBC_CONFIG_PASSWORD = "password";
  public static final String JDBC_CONFIG_PARTITIONCOLUMN ="partitioncolumn";
  public static final String JDBC_CONFIG_LOWERBOUND = "lowerbound";
  public static final String JDBC_CONFIG_UPPERBOUND = "upperbound";
  public static final String JDBC_CONFIG_NUMPARTITIONS = "numpartitions";

  private String url;
  private String tableName;
  private String username;
  private String password;
  private String partitionColumn;
  private long lowerBound;
  private long upperBound;
  private int numPartitions;

  @Override
  public void configure(Config config) {
    this.url = config.getString(JDBC_CONFIG_URL);
    this.tableName = config.getString(JDBC_CONFIG_TABLENAME);
    this.username = config.getString(JDBC_CONFIG_USERNAME);
    this.password = config.getString(JDBC_CONFIG_PASSWORD);
    this.partitionColumn = config.getString(JDBC_CONFIG_PARTITIONCOLUMN);
    this.lowerBound = config.getLong(JDBC_CONFIG_LOWERBOUND);
    this.upperBound = config.getLong(JDBC_CONFIG_UPPERBOUND);
    this.numPartitions = config.getInt(JDBC_CONFIG_NUMPARTITIONS);
  }

  @Override
  public Dataset<Row> read() throws Exception {
    Properties properties = new Properties();
    properties.put("user", username);
    properties.put("password", password);
    properties.put("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver");

    return Contexts.getSparkSession().read().jdbc(url, tableName, partitionColumn,lowerBound,upperBound,numPartitions, properties);
  }

  @Override
  public String getAlias() {
    return "jdbc";
  }

  @Override
  public Validations getValidations() {
    return Validations.builder()
        .mandatoryPath(JDBC_CONFIG_URL, ConfigValueType.STRING)
        .mandatoryPath(JDBC_CONFIG_TABLENAME, ConfigValueType.STRING)
        .mandatoryPath(JDBC_CONFIG_USERNAME, ConfigValueType.STRING)
        .mandatoryPath(JDBC_CONFIG_PASSWORD, ConfigValueType.STRING)
        .mandatoryPath(JDBC_CONFIG_PARTITIONCOLUMN, ConfigValueType.STRING)
        .mandatoryPath(JDBC_CONFIG_UPPERBOUND, ConfigValueType.NUMBER)
        .mandatoryPath(JDBC_CONFIG_LOWERBOUND, ConfigValueType.NUMBER)
        .mandatoryPath(JDBC_CONFIG_NUMPARTITIONS, ConfigValueType.NUMBER)
        .allowEmptyValue(JDBC_CONFIG_PASSWORD)
        .build();
  }
  
}
