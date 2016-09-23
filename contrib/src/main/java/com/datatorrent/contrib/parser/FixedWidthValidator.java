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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;


/**
 * Helper class with methods to validate data members to corresponding data types.
 */
public class FixedWidthValidator
{
  /**
   * List of accessors (setters) in order of fields in input JSON
   */
  List<com.datatorrent.contrib.parser.FixedWidthParser.TypeInfo> setters;
  /**
   * Schema is read into this object to access fields
   */
  private FixedWidthSchema schema;
  /**
   * Input Class for which an object is to be emitted if all validations pass
   */
  private Class<?> pojoClass;

  /**
   * Parameterized constructor to initialize data members
   * @param schema
   * @param pojoClass
   * @param setters
   */
  public FixedWidthValidator(FixedWidthSchema schema, Class<?> pojoClass, List<com.datatorrent.contrib.parser.FixedWidthParser.TypeInfo> setters)
  {
    this.schema = schema;
    this.pojoClass = pojoClass;
    this.setters = setters;
  }

  /**
   * Get the Schema
   * @return
   */
  public FixedWidthSchema getSchema()
  {
    return schema;
  }

  /**
   * Set the Schema
   * @param schema
   */
  public void setSchema(FixedWidthSchema schema)
  {
    this.schema = schema;
  }

  /**
   * Get the setters
   * @return
   */
  public List<com.datatorrent.contrib.parser.FixedWidthParser.TypeInfo> getSetters()
  {
    return setters;
  }

  /**
   * Set the Setters
   * @param setters
   */
  public void setSetters(List<FixedWidthParser.TypeInfo> setters)
  {
    this.setters = setters;
  }

  /**
   * Get the pojoClass
   * @return
   */
  public Class<?> getPojoClass()
  {
    return pojoClass;
  }

  /**
   * Set the pojoClass
   * @param pojoClass
   */
  public void setPojoClass(Class<?> pojoClass)
  {
    this.pojoClass = pojoClass;
  }

  /**
   * Function to validate Integer type data and set it in the Object
   * using accessors in TypeInfo
   * @param value
   * @param fieldName
   * @param typeInfo
   * @param pojoObject
   * @param toEmit
   */
  public void parseandsetIntegerValue(String value, String fieldName, FixedWidthParser.TypeInfo typeInfo, Object pojoObject, HashMap toEmit)
  {
    try {
      if (value != null && !value.isEmpty()) {
        int result = Integer.parseInt(value);
        toEmit.put(fieldName,result);
        if (typeInfo != null && pojoObject != null) {
          typeInfo.setter.set(pojoObject, result);
        }
      }
      else
        toEmit.put(fieldName,value);

    } catch (NumberFormatException e) {
      throw new RuntimeException("Error parsing" + value + " to Integer type", e);
    } catch (Exception e) {
      throw new RuntimeException("Error setting " + value + " in the given class" + typeInfo.toString(), e);

    }
  }

  /**
   * Function to validate Double type data and set it in the Object
   * using accessors in TypeInfo
   * @param value
   * @param fieldName
   * @param typeInfo
   * @param pojoObject
   * @param toEmit
   */
  public void parseandsetDoubleValue(String value, String fieldName, FixedWidthParser.TypeInfo typeInfo, Object pojoObject, HashMap toEmit)
  {
    try {

      if (value != null && !value.isEmpty()) {
        Double result = Double.parseDouble(value);
        toEmit.put(fieldName,result);
        if (typeInfo != null && pojoObject != null) {
          typeInfo.setter.set(pojoObject, result);
        }
      }
      else
        toEmit.put(fieldName, value);

    } catch (NumberFormatException e) {
      throw new RuntimeException("Error parsing" + value + " to Double type", e);
    } catch (Exception e) {
      throw new RuntimeException("Error setting " + value + " in the given class" + typeInfo.toString(), e);

    }
  }

  /**
   * Function to validate String type data and set it in the Object
   * using accessors in TypeInfo
   * @param value
   * @param fieldName
   * @param typeInfo
   * @param pojoObject
   * @param toEmit
   */
  public void parseandsetStringValue(String value, String fieldName, FixedWidthParser.TypeInfo typeInfo, Object pojoObject, HashMap toEmit)
  {
    try {
      toEmit.put(fieldName,value);
      if (value != null && !value.isEmpty()) {
        if (typeInfo != null && pojoObject != null) {
          typeInfo.setter.set(pojoObject, value);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Error setting " + value + " in the given class" + typeInfo.toString(), e);

    }
  }

  /**
   * Function to validate Character type data and set it in the Object
   * using accessors in TypeInfo
   * @param value
   * @param fieldName
   * @param typeInfo
   * @param pojoObject
   * @param toEmit
   */
  public void parseandsetCharacterValue(String value, String fieldName, FixedWidthParser.TypeInfo typeInfo, Object pojoObject, HashMap toEmit)
  {

    try {
      toEmit.put(fieldName,value);
      if (value != null && !value.isEmpty()) {
        if (typeInfo != null) {
          typeInfo.setter.set(pojoObject, value.charAt(0));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Error setting " + value + " in the given class" + typeInfo.toString(), e);

    }
  }

  /**
   * Function to validate Float type data and set it in the Object
   * using accessors in TypeInfo
   * @param value
   * @param fieldName
   * @param typeInfo
   * @param pojoObject
   * @param toEmit
   */
  public void parseandsetFloatValue(String value, String fieldName, FixedWidthParser.TypeInfo typeInfo, Object pojoObject, HashMap toEmit)
  {
    try {

      if (value != null && !value.isEmpty()) {
        Float result = Float.parseFloat(value);
        toEmit.put(fieldName,result);
        if (typeInfo != null && pojoObject != null) {
          typeInfo.setter.set(pojoObject, result);
        }
      }
      else
        toEmit.put(fieldName, value);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Error parsing" + value + " to Float type", e);
    } catch (Exception e) {
      throw new RuntimeException("Error setting " + value + " in the given class" + typeInfo.toString(), e);

    }
  }

  /**
   * Function to validate Long type data and set it in the Object
   * using accessors in TypeInfo
   * @param value
   * @param fieldName
   * @param typeInfo
   * @param pojoObject
   * @param toEmit
   */
  public void parseandsetLongValue(String value, String fieldName, FixedWidthParser.TypeInfo typeInfo, Object pojoObject, HashMap toEmit)
  {
    try {
      if (value != null && !value.isEmpty()) {
        Long result = Long.parseLong(value);
        toEmit.put(fieldName,result);
        if (typeInfo != null && pojoObject != null) {
          typeInfo.setter.set(pojoObject, result);
        }
      }
      else
        toEmit.put(fieldName, value);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Error parsing" + value + " to Long type", e);
    } catch (Exception e) {
      throw new RuntimeException("Error setting " + value + " in the given class" + typeInfo.toString(), e);

    }

  }

  /**
   *  Function to validate Short type data and set it in the Object
   * using accessors in TypeInfo
   * @param value
   * @param fieldName
   * @param typeInfo
   * @param pojoObject
   * @param toEmit
   */
  public void parseandsetShortValue(String value, String fieldName, FixedWidthParser.TypeInfo typeInfo, Object pojoObject, HashMap toEmit)
  {
    try {
      if (value != null && !value.isEmpty()) {
        Short result = Short.parseShort(value);
        toEmit.put(fieldName,result);
        if (typeInfo != null && pojoObject != null) {
          typeInfo.setter.set(pojoObject, result);
        }
      }
      else
        toEmit.put(fieldName, value);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Error parsing" + value + " to Short type", e);
    } catch (Exception e) {
      throw new RuntimeException("Error setting " + value + " in the given class" + typeInfo.toString(), e);

    }

  }

  /**
   * Function to validate Boolean type data and set it in the Object
   * using accessors in TypeInfo
   * @param value
   * @param fieldName
   * @param trueValue
   * @param falseValue
   * @param typeInfo
   * @param pojoObject
   * @param toEmit
   */
  public void parseandsetBooleanValue(String value, String fieldName, String trueValue, String falseValue, FixedWidthParser.TypeInfo typeInfo, Object pojoObject, HashMap toEmit)
  {
    try {
      if (value != null && !value.isEmpty()) {
        if (value.compareToIgnoreCase(trueValue) == 0) {
          toEmit.put(fieldName,Boolean.parseBoolean("true"));
          if (typeInfo != null) {
            typeInfo.setter.set(pojoObject, Boolean.parseBoolean("true"));

          }
        } else if (value.compareToIgnoreCase(falseValue) == 0) {
          toEmit.put(fieldName,Boolean.parseBoolean("false"));
          if (typeInfo != null) {
            typeInfo.setter.set(pojoObject, Boolean.parseBoolean("false"));

          }
        } else {
          throw new NumberFormatException();
        }
      }
      else
        toEmit.put(fieldName, value);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Error parsing" + value + " to Boolean type", e);
    } catch (Exception e) {
      throw new RuntimeException("Error setting " + value + " in the given class" + typeInfo.toString(), e);

    }
  }

  /**
   * Function to validate Date type data and set it in the Object
   * using accessors in TypeInfo
   * @param value
   * @param fieldName
   * @param dateFormat
   * @param typeInfo
   * @param pojoObject
   * @param toEmit
   */
  public void parseandsetDateValue(String value, String fieldName, String dateFormat, FixedWidthParser.TypeInfo typeInfo, Object pojoObject, HashMap toEmit)
  {
    try {
      if (value != null && !value.isEmpty()) {
        DateFormat df = new SimpleDateFormat(dateFormat);
        df.setLenient(false);
        Date result = df.parse(value);
        toEmit.put(fieldName,result);
        if (typeInfo != null && pojoObject != null) {
          typeInfo.setter.set(pojoObject, result);

        }
      }
      else
        toEmit.put(fieldName, value);
    } catch (ParseException e) {
      throw new RuntimeException("Error parsing" + value + " to Date type in format " + dateFormat, e);
    } catch (Exception e) {
      throw new RuntimeException("Error setting " + value + " in the given class" + typeInfo.toString(), e);
    }
  }

  /**
   * Function to parse and set the current substring to the corresponding type.
   * @param currentField
   * @param value
   * @param typeInfo
   * @param pojoObject
   * @param toEmit
   */
  public void parseandsetCurrentField(FixedWidthSchema.Field currentField, String value, FixedWidthParser.TypeInfo typeInfo, Object pojoObject, HashMap toEmit)
  {
    try {
      switch (currentField.getType()) {
        case INTEGER:
          parseandsetIntegerValue(value, currentField.getName(), typeInfo, pojoObject, toEmit);
          break;
        case DOUBLE:
          parseandsetDoubleValue(value, currentField.getName(), typeInfo, pojoObject, toEmit);
          break;
        case STRING:
          parseandsetStringValue(value, currentField.getName(), typeInfo, pojoObject, toEmit);
          break;
        case CHARACTER:
          parseandsetCharacterValue(value, currentField.getName(), typeInfo, pojoObject, toEmit);
          break;
        case FLOAT:
          parseandsetFloatValue(value, currentField.getName(), typeInfo, pojoObject, toEmit);
          break;
        case LONG:
          parseandsetLongValue(value, currentField.getName(), typeInfo, pojoObject, toEmit);
          break;
        case SHORT:
          parseandsetShortValue(value, currentField.getName(), typeInfo, pojoObject, toEmit);
          break;
        case BOOLEAN:
          parseandsetBooleanValue(value, currentField.getName(),currentField.getTrueValue(), currentField.getFalseValue(), typeInfo, pojoObject, toEmit);
          break;
        case DATE:
          parseandsetDateValue(value, currentField.getName(), currentField.getDateFormat(), typeInfo, pojoObject, toEmit);
          break;
        default:
          throw new RuntimeException("Invalid Type in Field", new Exception());
      }
    } catch (Exception e) {
      throw e;
    }

  }

  /**
   * Validates the input tuple and returns the POJO
   * @param values
   * @param toEmit
   * @return POJO
   */
  public Object validate(String[] values, HashMap toEmit)
  {
    Object pojoObject = null;
    try {
      List<FixedWidthSchema.Field> fields = schema.getFields();
      try {
        if (pojoClass != null) {
          pojoObject = pojoClass.newInstance();
        }
      } catch (InstantiationException ie) {
        pojoObject = null;
      }


      for (int i = 0; i < fields.size(); i++) {
        FixedWidthSchema.Field currentField = fields.get(i);
        FixedWidthParser.TypeInfo typeInfo = setters.get(i);
        parseandsetCurrentField(currentField,
          values[i], typeInfo, pojoObject, toEmit);
      }


    } catch (StringIndexOutOfBoundsException e) {
      throw new RuntimeException("Record length and tuple length mismatch ", e);
    } catch (IllegalAccessException ie) {
      throw new RuntimeException("Illegal Access ", ie);
    } catch (Exception e) {
      throw e;
    }
    return pojoObject;
  }
}
