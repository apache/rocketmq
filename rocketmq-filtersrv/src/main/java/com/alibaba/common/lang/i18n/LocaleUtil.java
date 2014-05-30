package com.alibaba.common.lang.i18n;

import com.alibaba.common.lang.StringUtil;

import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * 用来处理地域和字符编码的工具类。
 *
 * @author Michael Zhou
 * @version $Id: LocaleUtil.java 1149 2004-08-10 02:01:41Z baobao $
 */
public class LocaleUtil {
    /* ============================================================================ */
    /*  常量和singleton。                                                           */
    /* ============================================================================ */
    private static final Set         AVAILABLE_LANGUAGES     = new HashSet();
    private static final Set         AVAILABLE_COUNTRIES     = new HashSet();
    private static final LocaleInfo  systemLocaleInfo;
    private static LocaleInfo        defaultLocalInfo;
    private static final ThreadLocal contextLocaleInfoHolder = new ThreadLocal();

    static {
        // 取得所有可用的国家和语言。
        Locale[] availableLocales = Locale.getAvailableLocales();

        for (int i = 0; i < availableLocales.length; i++) {
            Locale locale = availableLocales[i];

            AVAILABLE_LANGUAGES.add(locale.getLanguage());
            AVAILABLE_COUNTRIES.add(locale.getCountry());
        }

        // 取得系统locale和charset。
        systemLocaleInfo     = new LocaleInfo();

        // 设置默认locale和charset。
        defaultLocalInfo = systemLocaleInfo;
    }

    /* ============================================================================ */
    /*  解析locale和localeInfo。                                                    */
    /* ============================================================================ */

    /**
     * 解析locale字符串。
     * 
     * <p>
     * Locale字符串是符合下列格式：<code>language_country_variant</code>。
     * </p>
     *
     * @param localeName 要解析的字符串
     *
     * @return <code>Locale</code>对象，如果locale字符串为空，则返回<code>null</code>
     */
    public static Locale parseLocale(String localeName) {
        if (localeName != null) {
            String[] localeParts = StringUtil.split(localeName, "_");
            int      len = localeParts.length;

            if (len > 0) {
                String language = localeParts[0];
                String country = "";
                String variant = "";

                if (len > 1) {
                    country = localeParts[1];
                }

                if (len > 2) {
                    variant = localeParts[2];
                }

                return new Locale(language, country, variant);
            }
        }

        return null;
    }

    /**
     * 解析并创建locale信息。
     *
     * @param localeName locale信息的字符串，包含locale和charset信息，以“:”分隔
     *
     * @return locale信息
     */
    public static LocaleInfo parseLocaleInfo(String localeName) {
        Locale locale  = null;
        String charset = null;

        if (StringUtil.isNotEmpty(localeName)) {
            int    index       = localeName.indexOf(":");
            String localePart  = localeName;
            String charsetPart = null;

            if (index >= 0) {
                localePart      = localeName.substring(0, index);
                charsetPart     = localeName.substring(index + 1);
            }

            // 解析locale。
            locale     = parseLocale(localePart);

            // 解析charset。
            charset = StringUtil.trimToNull(charsetPart);
        }

        return new LocaleInfo(locale, charset);
    }

    /* ============================================================================ */
    /*  有关locale和charset的辅助方法。                                             */
    /* ============================================================================ */

    /**
     * 判断locale是否被支持。
     *
     * @param locale 要检查的locale
     */
    public static boolean isLocaleSupported(Locale locale) {
        return (locale != null) && AVAILABLE_LANGUAGES.contains(locale.getLanguage())
        && AVAILABLE_COUNTRIES.contains(locale.getCountry());
    }

    /**
     * 判断指定的charset是否被支持。
     *
     * @param charset 要检查的charset
     */
    public static boolean isCharsetSupported(String charset) {
        return Charset.isSupported(charset);
    }

    /**
     * 取得正规的字符集名称, 如果指定字符集不存在, 则抛出<code>UnsupportedEncodingException</code>.
     *
     * @param charset 字符集名称
     *
     * @return 正规的字符集名称
     *
     * @throws IllegalCharsetNameException 如果指定字符集名称非法
     * @throws UnsupportedCharsetException 如果指定字符集不存在
     */
    public static String getCanonicalCharset(String charset) {
        return Charset.forName(charset).name();
    }

    /**
     * 取得正规的字符集名称, 如果指定字符集不存在, 则返回<code>null</code>.
     *
     * @param charset 字符集名称
     *
     * @return 正规的字符集名称, 如果指定字符集不存在, 则返回<code>null</code>
     */
    public static String getCanonicalCharset(String charset, String defaultCharset) {
        String result = null;

        try {
            result = getCanonicalCharset(charset);
        } catch (IllegalArgumentException e) {
            if (defaultCharset != null) {
                try {
                    result = getCanonicalCharset(defaultCharset);
                } catch (IllegalArgumentException ee) {
                }
            }
        }

        return result;
    }

    /**
     * 取得备选的resource bundle风格的名称列表。
     * 
     * <p>
     * 例如：<code>calculateBundleNames("hello.jsp", new Locale("zh", "CN", "variant"))</code>将返回下面列表：
     * 
     * <ol>
     * <li>
     * hello.jsp
     * </li>
     * <li>
     * hello_zh.jsp
     * </li>
     * <li>
     * hello_zh_CN.jsp
     * </li>
     * <li>
     * hello_zh_CN_variant.jsp
     * </li>
     * </ol>
     * </p>
     *
     * @param baseName bundle的基本名
     * @param locale 区域设置
     *
     * @return 所有备选的bundle名
     */
    public static List calculateBundleNames(String baseName, Locale locale) {
        return calculateBundleNames(baseName, locale, false);
    }

    /**
     * 取得备选的resource bundle风格的名称列表。
     * 
     * <p>
     * 例如：<code>calculateBundleNames("hello.jsp", new Locale("zh", "CN", "variant"),
     * false)</code>将返回下面列表：
     * 
     * <ol>
     * <li>
     * hello.jsp
     * </li>
     * <li>
     * hello_zh.jsp
     * </li>
     * <li>
     * hello_zh_CN.jsp
     * </li>
     * <li>
     * hello_zh_CN_variant.jsp
     * </li>
     * </ol>
     * </p>
     * 
     * <p>
     * 当<code>noext</code>为<code>true</code>时，不计算后缀名，例如<code>calculateBundleNames("hello.world",
     * new Locale("zh", "CN", "variant"), true)</code>将返回下面列表：
     * 
     * <ol>
     * <li>
     * hello.world
     * </li>
     * <li>
     * hello.world_zh
     * </li>
     * <li>
     * hello.world_zh_CN
     * </li>
     * <li>
     * hello.world_zh_CN_variant
     * </li>
     * </ol>
     * </p>
     *
     * @param baseName bundle的基本名
     * @param locale 区域设置
     *
     * @return 所有备选的bundle名
     */
    public static List calculateBundleNames(String baseName, Locale locale, boolean noext) {
        baseName = StringUtil.defaultIfEmpty(baseName);

        if (locale == null) {
            locale = new Locale("");
        }

        // 取后缀。
        String ext       = StringUtil.EMPTY_STRING;
        int    extLength = 0;

        if (!noext) {
            int extIndex = baseName.lastIndexOf(".");

            if (extIndex != -1) {
                ext           = baseName.substring(extIndex, baseName.length());
                extLength     = ext.length();
                baseName      = baseName.substring(0, extIndex);

                if (extLength == 1) {
                    ext           = StringUtil.EMPTY_STRING;
                    extLength     = 0;
                }
            }
        }

        // 计算locale后缀。
        List         result         = new ArrayList(4);
        String       language       = locale.getLanguage();
        int          languageLength = language.length();
        String       country        = locale.getCountry();
        int          countryLength  = country.length();
        String       variant        = locale.getVariant();
        int          variantLength  = variant.length();

        StringBuffer buffer = new StringBuffer(baseName);

        buffer.append(ext);
        result.add(buffer.toString());
        buffer.setLength(buffer.length() - extLength);

        // 如果locale是("", "", "").
        if ((languageLength + countryLength + variantLength) == 0) {
            return result;
        }

        // 加入baseName_language，如果baseName为空，则不加下划线。
        if (buffer.length() > 0) {
            buffer.append('_');
        }

        buffer.append(language);

        if (languageLength > 0) {
            buffer.append(ext);
            result.add(buffer.toString());
            buffer.setLength(buffer.length() - extLength);
        }

        if ((countryLength + variantLength) == 0) {
            return result;
        }

        // 加入baseName_language_country
        buffer.append('_').append(country);

        if (countryLength > 0) {
            buffer.append(ext);
            result.add(buffer.toString());
            buffer.setLength(buffer.length() - extLength);
        }

        if (variantLength == 0) {
            return result;
        }

        // 加入baseName_language_country_variant
        buffer.append('_').append(variant);

        buffer.append(ext);
        result.add(buffer.toString());
        buffer.setLength(buffer.length() - extLength);

        return result;
    }

    /* ============================================================================ */
    /*  取得系统范围的、默认的、线程范围的locale和charset。                         */
    /*                                                                              */
    /*   系统作用域： 由JVM所运行的操作系统环境决定，在JVM生命期内不改变。          */
    /*   默认作用域： 在整个JVM中全局有效，可被改变。默认值同“系统locale”。       */
    /*   线程作用域： 在整个线程中全局有效，可被改变。默认值同“默认locale”。      */
    /* ============================================================================ */

    /**
     * 取得操作系统默认的区域。
     *
     * @return 操作系统默认的区域
     */
    public static LocaleInfo getSystem() {
        return systemLocaleInfo;
    }

    /**
     * 取得默认的区域。
     *
     * @return 默认的区域
     */
    public static LocaleInfo getDefault() {
        return (defaultLocalInfo == null) ? systemLocaleInfo
                                          : defaultLocalInfo;
    }

    /**
     * 设置默认的区域。
     *
     * @param locale 区域
     *
     * @return 原来的默认区域
     */
    public static LocaleInfo setDefault(Locale locale) {
        LocaleInfo old = getDefault();

        defaultLocalInfo = new LocaleInfo(locale, null, systemLocaleInfo);

        return old;
    }

    /**
     * 设置默认的区域。
     *
     * @param locale 区域
     * @param charset 编码字符集
     *
     * @return 原来的默认区域
     */
    public static LocaleInfo setDefault(Locale locale, String charset) {
        LocaleInfo old = getDefault();

        defaultLocalInfo = new LocaleInfo(locale, charset, systemLocaleInfo);

        return old;
    }

    /**
     * 设置默认的区域。
     *
     * @param localeInfo 区域和编码字符集信息
     *
     * @return 原来的默认区域
     */
    public static LocaleInfo setDefault(LocaleInfo localeInfo) {
        if (localeInfo == null) {
            return setDefault(null, null);
        } else {
            LocaleInfo old = getDefault();

            defaultLocalInfo = localeInfo;

            return old;
        }
    }

    /**
     * 复位默认的区域设置。
     */
    public static void resetDefault() {
        defaultLocalInfo = systemLocaleInfo;
    }

    /**
     * 取得当前thread默认的区域。
     *
     * @return 当前thread默认的区域
     */
    public static LocaleInfo getContext() {
        LocaleInfo contextLocaleInfo = (LocaleInfo) contextLocaleInfoHolder.get();

        return (contextLocaleInfo == null) ? getDefault()
                                           : contextLocaleInfo;
    }

    /**
     * 设置当前thread默认的区域。
     *
     * @param locale 区域
     *
     * @return 原来的thread默认的区域
     */
    public static LocaleInfo setContext(Locale locale) {
        LocaleInfo old = getContext();

        contextLocaleInfoHolder.set(new LocaleInfo(locale, null, defaultLocalInfo));

        return old;
    }

    /**
     * 设置当前thread默认的区域。
     *
     * @param locale 区域
     * @param charset 编码字符集
     *
     * @return 原来的thread默认的区域
     */
    public static LocaleInfo setContext(Locale locale, String charset) {
        LocaleInfo old = getContext();

        contextLocaleInfoHolder.set(new LocaleInfo(locale, charset, defaultLocalInfo));

        return old;
    }

    /**
     * 设置当前thread默认的区域。
     *
     * @param localeInfo 区域和编码字符集信息
     *
     * @return 原来的thread默认的区域
     */
    public static LocaleInfo setContext(LocaleInfo localeInfo) {
        if (localeInfo == null) {
            return setContext(null, null);
        } else {
            LocaleInfo old = getContext();

            contextLocaleInfoHolder.set(localeInfo);

            return old;
        }
    }

    /**
     * 复位当前thread的区域设置。
     */
    public static void resetContext() {
        contextLocaleInfoHolder.set(null);
    }
}
