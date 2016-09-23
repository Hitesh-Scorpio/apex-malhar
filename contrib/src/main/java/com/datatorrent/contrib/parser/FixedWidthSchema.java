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

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * This is schema that defines fields and their constraints for fixed width files
 * The operators use this information to validate the incoming tuples.
 * Information from JSON schema is saved in this object and is used by the
 * operators
 * <p>
 * <br>
 * <br>
 * Example schema <br>
 * <br>
 * {{ "padding": " ",alignment="left","fields": [ {"name": "adId",
 * "type": "Integer","padding":"0", "fieldLength": 3} , { "name": "adName",
 * "type": "String", alignment: "right","fieldLength": 20}, { "name": "bidPrice", "type":
 * "Double", "fieldLength": 5}, { "name": "startDate", "type": "Date", "fieldLength": 10,
 * "format":"dd/MM/yyyy" }, { "name": "securityCode", "type": "Long","fieldLength": 10 },
 * { "name": "active", "type":"Boolean","fieldLength": 2} ] }}
 * @since 3.6.0
 */
public class FixedWidthSchema extends Schema
{

  /**
   * JSON key string for record length
   */
  private static final String FIELD_LEN = "length";
  /**
   * JSON key string for Global Padding Character
   */
  private static final String GLOBAL_PADDING_CHARACTER = "padding";
  /**
   * Default Padding Character
   */
  private static final char DEFAULT_PADDING_CHARACTER = ' ';
  /**
   * JSON key string for Global Alignment
   */
  private static final String GLOBAL_ALIGNMENT="alignment";
  /**
   * JSON key string for Default Alignment
   */
  private static final String DEFAULT_ALIGNMENT= "left";
  /**
   * JSON key string for Field Alignment
   */
  private static final String FIELD_ALIGNMENT ="alignment";
  /**
   * JSON key string for Field Padding
   */
  private static final String FIELD_PADDING ="padding";
  private static final Logger logger = LoggerFactory.getLogger(FixedWidthSchema.class);

  /**
   * This holds list of {@link Field}
   */
  private List<Field> fields = new LinkedList<Field>();
  /**
   * This holds the padding character
   */
  private char globalPadding;
  /**
   * This holds the global alignment
   */
  private String globalAlignment;

  /**
   * Constructor for FixedWidthSchema
   * @param json
   */
  public FixedWidthSchema(String json)
  {
    try {
      initialize(json);
    } catch (JSONException | IOException e) {
      logger.error("{}", e);
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Get the Padding character
   * @return
   */
  public char getGlobalPadding()
  {
    return globalPadding;
  }

  /**
   * Set the padding character
   * @param padding
   */
  public void setGlobalPadding(char padding)
  {
    this.globalPadding = padding;
  }

  /**
   * Get the global alignment
   * @return global alignment
   */
  public String getGlobalAlignment()
  {
    return globalAlignment;
  }

  /**
   * Set the global alignment
   * @param globalAlignment
   */
  public void setGlobalAlignment(String globalAlignment)
  {
    this.globalAlignment = globalAlignment;
  }

  /**
   * For a given json string, this method sets the field members
   *
   * @param json
   * @throws JSONException
   * @throws IOException
   */
  private void initialize(String json) throws JSONException, IOException
  {

    JSONObject jo = new JSONObject(json);
    JSONArray fieldArray = jo.getJSONArray(FIELDS);
    if (jo.has(GLOBAL_PADDING_CHARACTER)) {
      globalPadding = jo.getString(GLOBAL_PADDING_CHARACTER).charAt(0);
    } else {
      globalPadding = DEFAULT_PADDING_CHARACTER;
    }
    if (jo.has(GLOBAL_ALIGNMENT)) {
      globalAlignment = jo.getString(GLOBAL_ALIGNMENT);
    } else {
      globalAlignment = DEFAULT_ALIGNMENT;
    }

    for (int i = 0; i < fieldArray.length(); i++) {
      JSONObject obj = fieldArray.getJSONObject(i);
      Field field = new Field(obj.getString(NAME), obj.getString(TYPE).toUpperCase(), obj.getInt(FIELD_LEN));

      if(obj.has(FIELD_PADDING))
        field.setPadding(obj.getString(FIELD_PADDING).charAt(0));
      else
        field.setPadding(globalPadding);
      if(obj.has(FIELD_ALIGNMENT))
        field.setAlignment(obj.getString(FIELD_ALIGNMENT));
      else
        field.setAlignment(globalAlignment);

      //Get the format if the given data type is Date
      if (field.getType() == FieldType.DATE) {
        if (obj.has(DATE_FORMAT)) {
          field.setDateFormat(obj.getString(DATE_FORMAT));
        } else {
          field.setDateFormat(DEFAULT_DATE_FORMAT);
        }

      }
      //Get the trueValue and falseValue if the data type is Boolean
      if (field.getType() == FieldType.BOOLEAN) {
        if (obj.has(TRUE_VALUE)) {
          field.setTrueValue(obj.getString(TRUE_VALUE));
        } else {
          field.setTrueValue(DEFAULT_TRUE_VALUE);
        }
        if (obj.has(FALSE_VALUE)) {
          field.setFalseValue(obj.getString(FALSE_VALUE));
        } else {
          field.setFalseValue(DEFAULT_FALSE_VALUE);
        }

      }
      fields.add(field);
      fieldNames.add(field.name);

    }
  }

  /**
   * Get the list of Fields.
   *
   * @return fields
   */
  public List<Field> getFields()
  {
    return Collections.unmodifiableList(fields);
  }

  /**
   * Objects of this class represents a particular field in the schema. Each
   * field has a name, type and a fieldLength.
   * In case of type Date we need a dateFormat.
   *
   */
  public class Field extends Schema.Field
  {
    /**
     * Length of the field
     */
    private int fieldLength;
    /**
     * Parameter to specify format of date
     */
    private String dateFormat;
    /**
     * Parameter to specify true value of Boolean
     */
    private String trueValue;
    /**
     * Parameter to specify false value of Boolean
     */
    private String falseValue;
    /**
     * Parameter to specify padding
     */
    private char padding;
    /**
     * Parameter to specify alignment
     */
    private String alignment;

    /**
     * Constructor for Field
     * @param name
     * @param type
     * @param fieldLength
     */
    public Field(String name, String type, Integer fieldLength)
    {
      super(name, type);
      this.fieldLength = fieldLength;
      this.dateFormat = null;
      this.trueValue = null;
      this.falseValue = null;
      this.padding=' ';
      this.alignment=null;
    }

    /**
     * Get the Length of the Field
     * @return fieldLength
     */
    public int getFieldLength()
    {
      return fieldLength;
    }

    /**
     * Set the end pointer of the field
     *
     * @param fieldLength
     */
    public void setFieldLength(Integer fieldLength)
    {
      this.fieldLength = fieldLength;
    }

    /**
     * Get the dateFormat of the field
     *
     * @return dateFormat
     */
    public String getDateFormat()
    {
      return dateFormat;
    }

    /**
     * Set the the dateFormat of the field
     *
     * @param dateFormat
     */
    public void setDateFormat(String dateFormat)
    {
      this.dateFormat = dateFormat;
    }

    /**
     * Get the trueValue of the Boolean field
     * @return trueValue
     */
    public String getTrueValue()
    {
      return trueValue;
    }

    /**
     * Set the trueValue of the Boolean field
     *
     * @param trueValue
     */
    public void setTrueValue(String trueValue)
    {
      this.trueValue = trueValue;
    }

    /**
     * Get the falseValue of the Boolean field
     * @return falseValue
     */
    public String getFalseValue()
    {
      return falseValue;
    }

    /**
     * Set the end pointer of the field
     *
     * @param falseValue
     */
    public void setFalseValue(String falseValue)
    {
      this.falseValue = falseValue;
    }
    /**
     * Get the field padding
     * @return padding
     */
    public char getPadding()
    {
      return padding;
    }

    /**
     * Set the field padding
     * @param padding
     */
    public void setPadding(char padding)
    {
      this.padding = padding;
    }

    /**
     * Get the field alignment
     * @return alignment
     */
    public String getAlignment()
    {
      return alignment;
    }

    /**
     * Set the field alignment
     * @param alignment
     */
    public void setAlignment(String alignment)
    {
      this.alignment = alignment;
    }

    @Override
    public String toString()
    {
      return "Fields [name=" + name + ", type=" + type + " fieldLength= " + fieldLength + "]";
    }
  }

}
