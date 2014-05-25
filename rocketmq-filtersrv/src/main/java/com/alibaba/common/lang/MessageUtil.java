package com.alibaba.common.lang;

import java.text.MessageFormat;

import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * 和<code>ResourceBundle</code>及消息字符串有关的工具类。
 *
 * @author Michael Zhou
 * @version $Id: MessageUtil.java 1201 2004-12-09 01:39:09Z baobao $
 */
public class MessageUtil {
    /* ============================================================================ */
    /* 以下是有关resource bundle的方法                                              */
    /* ============================================================================ */

    /**
     * 从<code>ResourceBundle</code>中取得字符串，并使用<code>MessageFormat</code>格式化字符串.
     *
     * @param bundle resource bundle
     * @param key 要查找的键
     * @param params 参数表
     *
     * @return key对应的字符串，如果key为<code>null</code>或resource
     *         bundle为<code>null</code>，或resource key未找到，则返回<code>key</code>
     */
    public static String getMessage(ResourceBundle bundle, String key, Object[] params) {
        if ((bundle == null) || (key == null)) {
            return key;
        }

        try {
            String message = bundle.getString(key);

            return formatMessage(message, params);
        } catch (MissingResourceException e) {
            return key;
        }
    }

    /**
     * 从<code>ResourceBundle</code>中取得字符串，并使用<code>MessageFormat</code>格式化字符串.
     *
     * @param bundle resource bundle
     * @param key 要查找的键
     * @param param1 参数1
     *
     * @return key对应的字符串，如果key为<code>null</code>或resource
     *         bundle为<code>null</code>，则返回<code>null</code>。如果resource key未找到，则返回<code>key</code>
     */
    public static String getMessage(ResourceBundle bundle, String key, Object param1) {
        return getMessage(bundle, key, new Object[] { param1 });
    }

    /**
     * 从<code>ResourceBundle</code>中取得字符串，并使用<code>MessageFormat</code>格式化字符串.
     *
     * @param bundle resource bundle
     * @param key 要查找的键
     * @param param1 参数1
     * @param param2 参数2
     *
     * @return key对应的字符串，如果key为<code>null</code>或resource
     *         bundle为<code>null</code>，则返回<code>null</code>。如果resource key未找到，则返回<code>key</code>
     */
    public static String getMessage(ResourceBundle bundle, String key, Object param1, Object param2) {
        return getMessage(bundle, key, new Object[] { param1, param2 });
    }

    /**
     * 从<code>ResourceBundle</code>中取得字符串，并使用<code>MessageFormat</code>格式化字符串.
     *
     * @param bundle resource bundle
     * @param key 要查找的键
     * @param param1 参数1
     * @param param2 参数2
     * @param param3 参数3
     *
     * @return key对应的字符串，如果key为<code>null</code>或resource
     *         bundle为<code>null</code>，则返回<code>null</code>。如果resource key未找到，则返回<code>key</code>
     */
    public static String getMessage(ResourceBundle bundle, String key, Object param1,
        Object param2, Object param3) {
        return getMessage(bundle, key, new Object[] { param1, param2, param3 });
    }

    /**
     * 从<code>ResourceBundle</code>中取得字符串，并使用<code>MessageFormat</code>格式化字符串.
     *
     * @param bundle resource bundle
     * @param key 要查找的键
     * @param param1 参数1
     * @param param2 参数2
     * @param param3 参数3
     * @param param4 参数4
     *
     * @return key对应的字符串，如果key为<code>null</code>或resource
     *         bundle为<code>null</code>，则返回<code>null</code>。如果resource key未找到，则返回<code>key</code>
     */
    public static String getMessage(ResourceBundle bundle, String key, Object param1,
        Object param2, Object param3, Object param4) {
        return getMessage(bundle, key, new Object[] { param1, param2, param3, param4 });
    }

    /**
     * 从<code>ResourceBundle</code>中取得字符串，并使用<code>MessageFormat</code>格式化字符串.
     *
     * @param bundle resource bundle
     * @param key 要查找的键
     * @param param1 参数1
     * @param param2 参数2
     * @param param3 参数3
     * @param param4 参数4
     * @param param5 参数5
     *
     * @return key对应的字符串，如果key为<code>null</code>或resource
     *         bundle为<code>null</code>，则返回<code>null</code>。如果resource key未找到，则返回<code>key</code>
     */
    public static String getMessage(ResourceBundle bundle, String key, Object param1,
        Object param2, Object param3, Object param4, Object param5) {
        return getMessage(bundle, key, new Object[] { param1, param2, param3, param4, param5 });
    }

    /* ============================================================================ */
    /* 以下是用MessageFormat格式化字符串的方法                                      */
    /* ============================================================================ */

    /**
     * 使用<code>MessageFormat</code>格式化字符串.
     *
     * @param message 要格式化的字符串
     * @param params 参数表
     *
     * @return 格式化的字符串，如果message为<code>null</code>，则返回<code>null</code>
     */
    public static String formatMessage(String message, Object[] params) {
        if ((message == null) || (params == null) || (params.length == 0)) {
            return message;
        }

        return MessageFormat.format(message, params);
    }

    /**
     * 使用<code>MessageFormat</code>格式化字符串.
     *
     * @param message 要格式化的字符串
     * @param param1 参数1
     *
     * @return 格式化的字符串，如果message为<code>null</code>，则返回<code>null</code>
     */
    public static String formatMessage(String message, Object param1) {
        return formatMessage(message, new Object[] { param1 });
    }

    /**
     * 使用<code>MessageFormat</code>格式化字符串.
     *
     * @param message 要格式化的字符串
     * @param param1 参数1
     * @param param2 参数2
     *
     * @return 格式化的字符串，如果message为<code>null</code>，则返回<code>null</code>
     */
    public static String formatMessage(String message, Object param1, Object param2) {
        return formatMessage(message, new Object[] { param1, param2 });
    }

    /**
     * 使用<code>MessageFormat</code>格式化字符串.
     *
     * @param message 要格式化的字符串
     * @param param1 参数1
     * @param param2 参数2
     * @param param3 参数3
     *
     * @return 格式化的字符串，如果message为<code>null</code>，则返回<code>null</code>
     */
    public static String formatMessage(String message, Object param1, Object param2, Object param3) {
        return formatMessage(message, new Object[] { param1, param2, param3 });
    }

    /**
     * 使用<code>MessageFormat</code>格式化字符串.
     *
     * @param message 要格式化的字符串
     * @param param1 参数1
     * @param param2 参数2
     * @param param3 参数3
     * @param param4 参数4
     *
     * @return 格式化的字符串，如果message为<code>null</code>，则返回<code>null</code>
     */
    public static String formatMessage(String message, Object param1, Object param2, Object param3,
        Object param4) {
        return formatMessage(message, new Object[] { param1, param2, param3, param4 });
    }

    /**
     * 使用<code>MessageFormat</code>格式化字符串.
     *
     * @param message 要格式化的字符串
     * @param param1 参数1
     * @param param2 参数2
     * @param param3 参数3
     * @param param4 参数4
     * @param param5 参数5
     *
     * @return 格式化的字符串，如果message为<code>null</code>，则返回<code>null</code>
     */
    public static String formatMessage(String message, Object param1, Object param2, Object param3,
        Object param4, Object param5) {
        return formatMessage(message, new Object[] { param1, param2, param3, param4, param5 });
    }
}
