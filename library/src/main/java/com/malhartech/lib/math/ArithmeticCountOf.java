/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractModule;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.ModuleConfiguration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes in one stream via input port "data". A count is done on how many tuples satisfy the compare function. The function is given by
 * "key", "value", and "compare". If a tuple passed the test count is incremented. On end of window count iss emitted on the output port "countof".
 * The comparison is done by getting double value from the Number.<p>
 *  This module is an end of window module<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: Input port, expects HashMap<String, Object><br>
 * <b>countof</b>: Output port, emits Integer<br>
 * <br>
 * Properties:<br>
 * <b>key</b>: The key on which compare is done<br>
 * <b>value</b>: The value to compare with<br>
 * <b>compare<b>: The compare function. Supported values are "lte", "lt", "eq", "neq", "gt", "gte". Default is "eq"<br>
 * <br>
 * Compile time checks<br>
 * Key must be non empty<br>
 * Value must be able to convert to a "double"<br>
 * Compare string, if specified, must be one of "lte", "lt", "eq", "neq", "gt", "gte"<br>
 * <br>
 * Run time checks<br>
 * none<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Integer: ?? million tuples/s<br>
 * Double: ?? million tuples/s<br>
 * Long: ?? million tuples/s<br>
 * Short: ?? million tuples/s<br>
 * Float: ?? million tupels/s<br>
 *
 * @author amol
 */


@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = ArithmeticCountOf.IPORT_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = ArithmeticCountOf.OPORT_COUNTOF, type = PortAnnotation.PortType.OUTPUT)
})
public class ArithmeticCountOf extends AbstractModule
{
  public static final String IPORT_DATA = "data";
  public static final String OPORT_COUNTOF = "countof";
  private static Logger LOG = LoggerFactory.getLogger(ArithmeticCountOf.class);

  String key;
  double default_value = 0.0;
  double value = default_value;

  int count = 0;

  enum supported_type {LTE, LT, EQ, NEQ, GT, GTE};
  supported_type default_type = supported_type.EQ;
  supported_type type = default_type;

   /**
   * The key to compare on
   *
   */
  public static final String KEY_KEY = "key";

  /**
   * The value to compare with
   *
   */
  public static final String KEY_VALUE = "value";

  /**
   * The compare function
   *
   */
  public static final String KEY_COMPARE = "compare";

  /**
   * Process each tuple
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
    HashMap<String, Object> tuple = (HashMap<String, Object>) payload;
    Object val = tuple.get(key);
    double tvalue = 0;
    boolean errortuple = false;
    if (val != null) { // skip if key does not exist
      try {
        tvalue = Double.parseDouble(val.toString());
      }
      catch (NumberFormatException e) {
        errortuple = true;
      }
      if (!errortuple) {
        if (((type == supported_type.LT) && (tvalue < value))
                || ((type == supported_type.LTE) && (tvalue <= value))
                || ((type == supported_type.EQ) && (tvalue == value))
                || ((type == supported_type.NEQ) && (tvalue != value))
                || ((type == supported_type.GT) && (tvalue > value))
                || ((type == supported_type.GTE) && (tvalue >= value))) {
          count++;
        }
      }
      else { // emit error tuple, the string has to be Double
        ;
      }
    }
    else { // is this an error condition?
      ;
    }
  }

  @Override
  public void beginWindow()
  {
     count = 0;
  }

  @Override
  public void endWindow()
  {
    emit(new Integer(count));
  }

  public boolean myValidation(ModuleConfiguration config)
  {
    boolean ret = true;

    String key = config.get(KEY_KEY, "");
    String vstr = config.get(KEY_VALUE, "");
    String cstr = config.get(KEY_COMPARE, "");

    if (key.isEmpty()) {
      ret = false;
      throw new IllegalArgumentException(String.format("Property \"%s\" has to be specified", KEY_KEY));
    }

    try {
      double value = Double.parseDouble(vstr);
    }
    catch (NumberFormatException e) {
      ret = false;
      throw new IllegalArgumentException(String.format("Property \"%s\" is not a valid number", KEY_VALUE, vstr));
    }

    if (!cstr.isEmpty() && !cstr.equals("lt") && !cstr.equals("lte") && !cstr.equals("eq") && !cstr.equals("neq") && !cstr.equals("gt") && !cstr.equals("gte")) {
      ret = false;
      throw new IllegalArgumentException(String.format("Property \"%s\" is \"%s\". Valid values are \"lte\", \"lt\", \"eq\", \"neq\", \"gt\", \"gte\"", KEY_COMPARE, cstr));
    }
    return ret;
  }
   /**
   *
   * @param config
   */
  @Override
  public void setup(ModuleConfiguration config) throws FailedOperationException
  {
    if (!myValidation(config)) {
      throw new FailedOperationException("validation failed");
    }

    key = config.get(KEY_KEY);
    String vstr = config.get(KEY_VALUE);
    String cstr = config.get(KEY_COMPARE, "lt");

    double value = Double.parseDouble(vstr);
    if (cstr.equals("lt")) {
      type = supported_type.LT;
    }
    else if (cstr.equals("lte")) {
      type = supported_type.LTE;
    }
    else if (cstr.equals("eq")) {
      type = supported_type.EQ;
    }
    else if (cstr.equals("neq")) {
      type = supported_type.NEQ;
    }
    else if (cstr.equals("gt")) {
      type = supported_type.GT;
    }
    else if (cstr.equals("gte")) {
      type = supported_type.GTE;
    }
    else {
      type = supported_type.EQ;
    }
    LOG.debug(String.format("Set up: \"%s\" \"%s\" \"%s\"", key, cstr, value));
  }


  /**
   *
   * Checks for user specific configuration values<p>
   *
   * @param config
   * @return boolean
   */
  @Override
  public boolean checkConfiguration(ModuleConfiguration config)
  {
    boolean ret = true;
    // TBD
    return ret && super.checkConfiguration(config);
  }
}
