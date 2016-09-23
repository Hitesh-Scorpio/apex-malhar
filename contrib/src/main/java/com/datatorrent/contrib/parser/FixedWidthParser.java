/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.datatorrent.contrib.parser;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ClassUtils;

import com.google.common.annotations.VisibleForTesting;
import com.univocity.parsers.fixed.FieldAlignment;
import com.univocity.parsers.fixed.FixedWidthFields;
import com.univocity.parsers.fixed.FixedWidthParserSettings;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.parser.Parser;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.PojoUtils;

/**
 * Operator that parses a fixed width record against a specified schema <br>
 * Schema is specified in a json format as per {@link FixedWidthSchema} that
 * contains field information for each field.<br>
 * Assumption is that each field in the data should map to a simple
 * java type.<br>
 * <br>
 * <b>Properties</b> <br>
 * <b>schema</b>:schema as a string<br>
 * <b>clazz</b>:Pojo class <br>
 * <b>Ports</b> <br>
 * <b>in</b>:input tuple as a byte array. Each tuple represents a record<br>
 * <b>parsedOutput</b>:tuples that are validated against the schema are emitted
 * as HashMap<String,Object> on this port<br>
 * Key being the name of the field and Val being the value of the field.
 * <b>out</b>:tuples that are validated against the schema are emitted as pojo
 * on this port<br>
 * <b>err</b>:tuples that do not confine to schema are emitted on this port as
 * KeyValPair<String,String><br>
 * Key being the tuple and Val being the reason.
 *
 * @displayName FixedWidthParser
 * @category Parsers
 * @tags fixedwidth pojo parser
 * @since 3.6.0
 */
public class FixedWidthParser extends Parser<byte[], KeyValPair<String, String>> implements Operator.ActivationListener<Context>
{
  private static final Logger logger = LoggerFactory.getLogger(FixedWidthParser.class);
  public final transient DefaultOutputPort<HashMap<String, Object>> parsedOutput = new DefaultOutputPort<HashMap<String, Object>>();
  /**
   * Metric to keep count of number of tuples emitted on {@link #parsedOutput}
   * port
   */
  @AutoMetric
  private long parsedOutputCount;
  /**
   * Contents of the schema.Schema is specified in a json format as per
   * {@link FixedWidthSchema}
   */
  @NotNull
  private String jsonSchema;
  /**
   * Total length of the record
   */
  private int recordLength;
  /**
   * Schema is read into this object to access fields
   */
  private transient FixedWidthSchema schema;
  /**
   * FixedWidthValidator is used for data type conversions and validations
   * It is also responsible for creating the POJO to be emitted
   */
  private transient FixedWidthValidator fixedWidthValidator;
  /**
   * List of setters to set the value in POJO to be emitted
   */
  private transient List<FixedWidthParser.TypeInfo> setters;
  /**
   * header- This will be string of field names, padded with padding character (if required)
   */
  private transient String header;
  /**
   * Univocity Parser to parse the input tuples
   */
  private com.univocity.parsers.fixed.FixedWidthParser univocityParser;

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    parsedOutputCount = 0;
  }

  @Override
  public void processTuple(byte[] tuple)
  {
    if (tuple == null) {
      if (err.isConnected()) {
        err.emit(new KeyValPair<String, String>(null, "Blank/null tuple"));
        logger.error("Tuple could not be parsed. Reason Blank/null tuple");
      }
      errorTupleCount++;
      return;
    }
    String incomingString = new String(tuple);
    if (StringUtils.isBlank(incomingString) || StringUtils.equals(incomingString, getHeader())) {
      if (err.isConnected()) {
        err.emit(new KeyValPair<String, String>(incomingString, "Blank/header tuple"));
        logger.error("Tuple could not be parsed. Reason Blank/header tuple");
      }
      errorTupleCount++;
      return;
    }
    if (incomingString.length() < recordLength) {
      if (err.isConnected()) {
        err.emit(new KeyValPair<String, String>(incomingString, "Record length mis-match/shorter tuple"));
        logger.error("Tuple could not be parsed. Reason Record length mis-match/shorter tuple");
      }
      errorTupleCount++;
      return;
    }
    if (incomingString.length() > recordLength) {
      if (err.isConnected()) {
        err.emit(new KeyValPair<String, String>(incomingString, "Record length mis-match/longer tuple"));
        logger.error("Tuple could not be parsed. Reason Record length mis-match/longer tuple");
      }
      errorTupleCount++;
      return;
    }
    try {
      String[] values = univocityParser.parseLine(incomingString);
      HashMap<String, Object> toEmit = new HashMap();
      Object pojo = fixedWidthValidator.validate(values, toEmit);
      if (parsedOutput.isConnected()) {
        parsedOutput.emit(toEmit);
        parsedOutputCount++;
      }
      if (out.isConnected() && clazz != null) {
        out.emit(pojo);
        emittedObjectCount++;
      }
    } catch (Exception e) {
      if (err.isConnected()) {
        err.emit(new KeyValPair<String, String>(incomingString, e.getMessage()));
      }
      errorTupleCount++;
      logger.error("Tuple could not be parsed. Reason {}", e.getMessage());
    }

  }

  @Override
  public KeyValPair<String, String> processErrorTuple(byte[] input)
  {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public Object convert(byte[] tuple)
  {
    throw new UnsupportedOperationException("Not supported");
  }

  /**
   * Setup the parser
   *
   * @param context
   */
  @Override
  public void setup(Context.OperatorContext context)
  {
    try {
      schema = new FixedWidthSchema(jsonSchema);
      fixedWidthValidator = new FixedWidthValidator(schema, null, null);
      recordLength = 0;
      List<FixedWidthSchema.Field> fields = schema.getFields();
      for (int i = 0; i < fields.size(); i++) {
        recordLength += fields.get(i).getFieldLength();
      }
      createUnivocityParser();
    } catch (Exception e) {
      logger.error("Cannot setup Parser Reason {}", e.getMessage());
    }
  }

  /**
   * Activate the Parser
   * Initialize the validator
   *
   * @param context
   */
  @Override
  public void activate(Context context)
  {
    try {
      if (clazz != null) {
        setters = new ArrayList<>();
        List<String> fieldNames = schema.getFieldNames();
        if (fieldNames != null) {
          for (String fieldName : fieldNames) {
            addSetter(fieldName);
          }
        }

        fixedWidthValidator.setPojoClass(clazz);
        fixedWidthValidator.setSetters(setters);
      }
    } catch (Exception e) {
      logger.error("Cannot activate Parser Reason {}", e.getMessage());
    }

  }

  /**
   * Function to create a univocity Parser
   */
  public void createUnivocityParser()
  {
    List<FixedWidthSchema.Field> fields = schema.getFields();
    FixedWidthFields fieldWidth = new FixedWidthFields();

    for (int i = 0; i < fields.size(); i++) {
      FixedWidthSchema.Field currentField = fields.get(i);
      int fieldLength = currentField.getFieldLength();
      FieldAlignment currentFieldAlignment;

      if (currentField.getAlignment().equalsIgnoreCase("centre")) {
        currentFieldAlignment = FieldAlignment.CENTER;
      } else if (currentField.getAlignment().equalsIgnoreCase("left")) {
        currentFieldAlignment = FieldAlignment.LEFT;
      } else {
        currentFieldAlignment = FieldAlignment.RIGHT;
      }
      fieldWidth.addField(currentField.getName(), fieldLength, currentFieldAlignment, currentField.getPadding());

    }

    FixedWidthParserSettings settings = new FixedWidthParserSettings(fieldWidth);
    univocityParser = new com.univocity.parsers.fixed.FixedWidthParser(settings);

  }

  @Override
  public void deactivate()
  {

  }

  /**
   * Function to add create a setter for a field and add it
   * to the List of setters
   *
   * @param fieldName
   */
  public void addSetter(String fieldName)
  {
    try {
      Field f = clazz.getDeclaredField(fieldName);
      FixedWidthParser.TypeInfo t = new FixedWidthParser.TypeInfo(f.getName(),
        ClassUtils.primitiveToWrapper(f.getType()));
      t.setter = PojoUtils.createSetter(clazz, t.name, t.type);
      setters.add(t);

    } catch (NoSuchFieldException e) {
      throw new RuntimeException("Field " + fieldName + " not found in class " + clazz, e);
    } catch (Exception e) {
      throw e;
    }
  }

  /**
   * Get the schema
   *
   * @return
   */
  public String getJsonSchema()
  {
    return jsonSchema;
  }

  /**
   * Set the jsonSchema
   *
   * @param jsonSchema
   */
  public void setJsonSchema(String jsonSchema)
  {
    this.jsonSchema = jsonSchema;
  }

  /**
   * Get the header
   *
   * @return
   */
  public String getHeader()
  {
    return header;
  }

  /**
   * Set the header
   *
   * @param header
   */
  public void setHeader(String header)
  {
    this.header = header;
  }

  /**
   * Get errorTupleCount
   *
   * @return errorTupleCount
   */
  @VisibleForTesting
  public long getErrorTupleCount()
  {
    return errorTupleCount;
  }

  /**
   * Get emittedObjectCount
   *
   * @return emittedObjectCount
   */
  @VisibleForTesting
  public long getEmittedObjectCount()
  {
    return emittedObjectCount;
  }

  /**
   * Get incomingTuplesCount
   *
   * @return incomingTuplesCount
   */
  @VisibleForTesting
  public long getIncomingTuplesCount()
  {
    return incomingTuplesCount;
  }

  /**
   * Get parsedOutputCount
   *
   * @return parsedOutPutCount
   */
  @VisibleForTesting
  public long getParsedOutputCount()
  {
    return parsedOutputCount;
  }

  /**
   * Objects of this class represents a particular data member of the Class to be emitted.
   * Each data member  has a name, type and a accessor(setter) function associated with it.
   */
  static class TypeInfo
  {
    String name;
    Class type;
    PojoUtils.Setter setter;

    public TypeInfo(String name, Class<?> type)
    {
      this.name = name;
      this.type = type;
    }

    public String toString()
    {
      String s = new String("'name': " + name + " 'type': " + type);
      return s;
    }
  }

}
