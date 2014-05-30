package com.alibaba.common.lang.enumeration;

import com.alibaba.common.lang.ClassLoaderUtil;
import com.alibaba.common.lang.StringUtil;
import com.alibaba.common.lang.enumeration.internal.EnumConstant;
import com.alibaba.common.lang.enumeration.internal.NumberType;

import java.io.InvalidClassException;
import java.io.ObjectStreamException;
import java.io.Serializable;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import java.text.MessageFormat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 类型安全的枚举类型.
 *
 * @author Michael Zhou
 * @version $Id: Enum.java 1749 2006-07-20 02:04:18Z baobao $
 */
public abstract class Enum extends Number implements NumberType, Comparable, Serializable {
    private static final long serialVersionUID = -3420208858441821772L;
    private String            name;
    private Number            value;

    /* ============================================================================ */
    /*  创建enum和初始化函数。                                                      */
    /* ============================================================================ */
/**
     * 创建一个枚举量。
     */
    protected Enum() {
    }

    /**
     * 创建一个枚举量。<p>枚举量的名称和常量名称相同，而值将被自动产生。</p>
     */
    protected static final Enum create() {
        return createEnum(null, null, false);
    }

    /**
     * 创建一个枚举量。<p>枚举量的名称和常量名称相同，而值将被自动产生。</p>
     *
     * @param name 枚举量的名称
     */
    protected static final Enum create(String name) {
        return createEnum(name, null, false);
    }

    /**
     * 创建一个枚举量, 并赋予指定的值.
     *
     * @param value 枚举量的值, 这个值不能为<code>null</code>
     */
    static final Enum createEnum(Number value) {
        return createEnum(null, value, true);
    }

    /**
     * 创建一个枚举量, 并赋予指定的值.
     *
     * @param name 枚举量的名称
     * @param value 枚举量的值, 这个值不能为<code>null</code>
     */
    static final Enum createEnum(String name, Number value) {
        return createEnum(name, value, true);
    }

    /**
     * 创建一个枚举量.
     *
     * @param name 枚举量的名称
     * @param value 枚举量的值
     * @param withValue 如果是<code>true</code>, 则该枚举量被赋予指定的值, 否则该枚举量将被赋予一个自动产生的值
     */
    private static Enum createEnum(String name, Number value, boolean withValue) {
        String enumClassName                   = null;
        Class  enumClass;
        Enum   enumObject;

        try {
            enumClassName                      = getCallerClassName();
            enumClass                          = ClassLoaderUtil.loadClass(enumClassName);
            enumObject                         = (Enum) enumClass.newInstance();

            enumObject.setName(StringUtil.trimToNull(name));
        } catch (ClassNotFoundException e) {
            throw new CreateEnumException("Could not find enum class " + enumClassName, e);
        } catch (Exception e) {
            throw new CreateEnumException("Could not instantiate enum instance of class "
                                          + enumClassName, e);
        }

        if (withValue && (value == null)) {
            throw new NullPointerException(EnumConstant.ENUM_VALUE_IS_NULL);
        }

        EnumType enumType = EnumUtil.getEnumType(enumClass);
        boolean  flagMode = enumObject instanceof Flags;

        if (withValue) {
            enumObject.value = enumType.setValue(value, flagMode);
        } else {
            enumObject.value = enumType.getNextValue(flagMode);
        }

        // 将enum加入enumList。
        enumType.enumList.add(enumObject);

        // 将enum加入valueMap, 如果有多个enum的值相同, 则取第一个
        if (!enumType.valueMap.containsKey(enumObject.value)) {
            enumType.valueMap.put(enumObject.value, enumObject);
        }

        // 将enum加入nameMap, 如果有多个enum的名字相同, 则取第一个
        if ((enumObject.name != null) && !enumType.nameMap.containsKey(enumObject.name)) {
            enumType.nameMap.put(enumObject.name, enumObject);
        }

        return enumObject;
    }

    /**
     * 取得调用者的类名。
     *
     * @return 调用者类名
     */
    private static String getCallerClassName() {
        StackTraceElement[] callers   = new Throwable().getStackTrace();
        String              enumClass = Enum.class.getName();

        for (int i = 0; i < callers.length; i++) {
            StackTraceElement caller     = callers[i];
            String            className  = caller.getClassName();
            String            methodName = caller.getMethodName();

            if (!enumClass.equals(className) && "<clinit>".equals(methodName)) {
                return className;
            }
        }

        throw new CreateEnumException("Cannot get Enum-class name");
    }

    /* ============================================================================ */
    /*  Enum成员函数。                                                              */
    /* ============================================================================ */
    /**
     * 取得枚举量的名称.
     *
     * @return 枚举量的名称
     */
    public String getName() {
        if (name == null) {
            Class    enumClass = ensureClassLoaded();
            EnumType enumType  = EnumUtil.getEnumType(enumClass);

            enumType.populateNames(enumClass);
        }

        return name;
    }

    /**
     * 设置枚举量的名称。<p>如果名称已经被设置，该方法将抛出异常。</p>
     *
     * @param name 枚举量的名称
     *
     * @return 当前enum
     *
     * @throws IllegalStateException 如果名称已经被设置
     */
    private Enum setName(String name) {
        if (this.name != null) {
            throw new IllegalStateException("Enum name already set: " + this.name);
        }

        this.name = name;

        return this;
    }

    /**
     * 取得枚举量的值.
     *
     * @return 枚举量的值
     */
    public Number getValue() {
        return value;
    }

    /**
     * 实现<code>Number</code>类, 取得<code>byte</code>值.
     *
     * @return <code>byte</code>值
     */
    public byte byteValue() {
        return (byte) intValue();
    }

    /**
     * 实现<code>Number</code>类, 取得<code>short</code>值.
     *
     * @return <code>short</code>值
     */
    public short shortValue() {
        return (short) intValue();
    }

    /**
     * 和另一个枚举量比较大小, 就是按枚举量的值比较.
     *
     * @param otherEnum 要比较的枚举量
     *
     * @return 如果等于<code>0</code>, 表示值相等, 大于<code>0</code>表示当前的枚举量的值比<code>otherEnum</code>大,
     *         小于<code>0</code>表示当前的枚举量的值比<code>otherEnum</code>小
     */
    public int compareTo(Object otherEnum) {
        if (!getClass().equals(otherEnum.getClass())) {
            throw new CreateEnumException(MessageFormat.format(EnumConstant.COMPARE_TYPE_MISMATCH,
                                                               new Object[] {
                                                                   getClass().getName(),
                                                                   otherEnum.getClass().getName()
                                                               }));
        }

        return ((Comparable) value).compareTo(((Enum) otherEnum).value);
    }

    /**
     * 比较两个枚举量是否相等, 即: 类型相同, 并且值相同(但名字可以不同).
     *
     * @param obj 要比较的对象
     *
     * @return 如果相等, 则返回<code>true</code>
     */
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if ((obj == null) || !getClass().equals(obj.getClass())) {
            return false;
        }

        return value.equals(((Enum) obj).value);
    }

    /**
     * 取得枚举量的hash值.  如果两个枚举量相同, 则它们的hash值一定相同.
     *
     * @return hash值
     */
    public int hashCode() {
        return getClass().hashCode() ^ value.hashCode();
    }

    /**
     * 将枚举量转换成字符串, 也就是枚举量的名称.
     *
     * @return 枚举量的名称
     */
    public String toString() {
        return getName();
    }

    /**
     * 取得已被装载的class。
     *
     * @return 当前对象的class
     */
    public Class ensureClassLoaded() {
        Class enumClass = getClass();

        synchronized (enumClass) {
            return enumClass;
        }
    }

    /**
     * 被"序列化"过程调用, 返回枚举量的singleton.
     *
     * @return 枚举量的singleton
     *
     * @throws ObjectStreamException 如果序列化出错
     */
    protected Object writeReplace() throws ObjectStreamException {
        getName();
        return this;
    }

    /**
     * 被"反序列化"过程调用, 确保返回枚举量的singleton.
     *
     * @return 枚举量的singleton
     *
     * @throws ObjectStreamException 如果反序列化出错
     */
    protected Object readResolve() throws ObjectStreamException {
        Class    enumClass  = ensureClassLoaded();
        EnumType enumType   = EnumUtil.getEnumType(enumClass);
        Enum     enumObject = (Enum) enumType.nameMap.get(getName());

        if (enumObject == null) {
            enumType.populateNames(enumClass);
            enumObject = (Enum) enumType.nameMap.get(getName());
        }

        if (enumObject == null) {
            throw new InvalidClassException("Enum name \"" + getName() + "\" not found in class "
                                            + enumClass.getName());
        }

        if (!enumObject.value.equals(value)) {
            throw new InvalidClassException("Enum value \"" + value + "\" does not match in class "
                                            + enumClass.getName());
        }

        return enumObject;
    }

    /**
     * 代表一个枚举类型的额外信息.
     */
    protected abstract static class EnumType {
        private Number value;
        final Map      nameMap  = Collections.synchronizedMap(new HashMap());
        final Map      valueMap = Collections.synchronizedMap(new HashMap());
        final List     enumList = new ArrayList();

        /**
         * 设置指定值为当前值.
         *
         * @param value 当前值
         * @param flagMode 是否为位模式
         *
         * @return 当前值
         */
        final Number setValue(Number value, boolean flagMode) {
            this.value = value;

            return value;
        }

        /**
         * 取得下一个值.
         *
         * @param flagMode 是否为位模式
         *
         * @return 当前值
         */
        final Number getNextValue(boolean flagMode) {
            value = getNextValue(value, flagMode);

            if (flagMode && isZero(value)) {
                throw new UnsupportedOperationException(EnumConstant.VALUE_OUT_OF_RANGE);
            }

            return value;
        }

        /**
         * 使用反射的方式装配enum的名称。
         *
         * @param enumClass enum类
         */
        final void populateNames(Class enumClass) {
            synchronized (enumClass) {
                Field[] fields = enumClass.getFields();

                for (int i = 0; i < fields.length; i++) {
                    Field field    = fields[i];
                    int   modifier = field.getModifiers();

                    if (Modifier.isPublic(modifier) && Modifier.isFinal(modifier)
                                && Modifier.isStatic(modifier)) {
                        try {
                            Object value = field.get(null);

                            for (Iterator j = valueMap.values().iterator(); j.hasNext();) {
                                Enum enumObject = (Enum) j.next();

                                if ((value == enumObject) && (enumObject.name == null)) {
                                    enumObject.name = field.getName();
                                    nameMap.put(enumObject.name, enumObject);
                                    break;
                                }
                            }
                        } catch (IllegalAccessException e) {
                            throw new CreateEnumException(e);
                        }
                    }
                }
            }
        }

        /**
         * 取得<code>Enum</code>值的类型.
         *
         * @return <code>Enum</code>值的类型
         */
        protected abstract Class getUnderlyingClass();

        /**
         * 取得指定值的下一个值.
         *
         * @param value 指定值
         * @param flagMode 是否为位模式
         *
         * @return 如果<code>value</code>为<code>null</code>, 则返回默认的初始值, 否则返回下一个值
         */
        protected abstract Number getNextValue(Number value, boolean flagMode);

        /**
         * 判断是否为<code>0</code>.
         *
         * @param value 要判断的值
         *
         * @return 如果是, 则返回<code>true</code>
         */
        protected abstract boolean isZero(Number value);
    }
}
