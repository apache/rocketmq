package com.alibaba.common.lang.enumeration.internal;

/**
 * 定义<code>Enum</code>相关的常量和错误信息.
 *
 * @author Michael Zhou
 * @version $Id: EnumConstant.java 1196 2004-11-24 01:03:13Z baobao $
 */
public interface EnumConstant {
    String CREATE_ENUM_TYPE_METHOD_NAME = "createEnumType";
    String FLAG_SET_INNER_CLASS_NAME = "FlagSet";

    // Enum的出错信息.
    String ENUM_NAME_IS_EMPTY             = "The Enum name must not be empty";
    String ENUM_VALUE_IS_NULL             = "The Enum value must not be null";
    String DUPLICATED_ENUM_NAME           = "Duplicated name \"{0}\" of Enum class \"{1}\"";
    String ENUM_CLASS_IS_NULL             = "The Enum class must not be null";
    String CLASS_IS_NOT_ENUM              = "Class \"{0}\" is not a subclass of Enum";
    String FAILED_CREATING_ENUM_TYPE      = "Could not create EnumType for class \"{0}\"";
    String COMPARE_TYPE_MISMATCH          = "Could not compare object of \"{0}\" with object of \"{1}\"";
    String VALUE_OUT_OF_RANGE             = "The flag value is out of range";
    String ENUM_IS_NOT_A_FLAG             = "The Enum class \"{0}\" is not an implementation of Flags";
    String CREATE_FLAG_SET_IS_UNSUPPORTED = "Creating FlagSet is not supported by Enum class \"{0}\".  Make sure there is a static inner class \"{0}.FlagSet\" with a constructor without parameters";

    // FlagSet的出错信息
    String ILLEGAL_CLASS                     = "Class \"{0}\" is not a subclass of \"{1}\"";
    String ILLEGAL_INTERFACE                 = "Class \"{0}\" is not an implementation of \"{1}\"";
    String CLONE_NOT_SUPPORTED               = "Clone not supported by class \"{0}\"";
    String COMPARE_UNDERLYING_CLASS_MISMATCH = "Could not compare FlagSet of underlying class \"{0}\" with FlagSet of underlying class \"{1}\"";
    String FLAGS_IS_NULL                     = "Flags must not be null";
    String FLAT_SET_VALUE_IS_NULL            = "FlagSet value must not be null";
    String ILLEGAL_FLAGS_OBJECT              = "Flags must be of \"{0}\" or \"{1}\"";
    String FLAG_SET_IS_IMMUTABLE             = "FlagSet is immutable";
}
