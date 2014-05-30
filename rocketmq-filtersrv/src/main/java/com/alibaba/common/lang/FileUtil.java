package com.alibaba.common.lang;

import java.io.File;

import java.net.URL;

import java.util.StringTokenizer;

/**
 * 处理文件的工具类。
 *
 * @author Michael Zhou
 * @version $Id: FileUtil.java 900 2004-04-19 01:15:33Z baobao $
 */
public class FileUtil {
    /* ============================================================================ */
    /*  常量和singleton。                                                           */
    /* ============================================================================ */
    private static final char   COLON_CHAR     = ':';
    private static final String UNC_PREFIX     = "//";
    private static final String SLASH          = "/";
    private static final String BACKSLASH      = "\\";
    private static final char   SLASH_CHAR     = '/';
    private static final char   BACKSLASH_CHAR = '\\';
    private static final String ALL_SLASH      = "/\\";

    /** 后缀分隔符。 */
    public static final String EXTENSION_SEPARATOR = ".";

    /** 当前目录记号："." */
    public static final String CURRENT_DIR = ".";

    /** 上级目录记号：".." */
    public static final String UP_LEVEL_DIR = "..";

    /* ============================================================================ */
    /*  规格化路径。                                                                */
    /*                                                                              */
    /*  去除'.'和'..'，支持windows路径和UNC路径。                                   */
    /* ============================================================================ */

    /**
     * 规格化路径。
     * 
     * <p>
     * 该方法忽略操作系统的类型，并是返回以“<code>/</code>”开始的绝对路径。转换规则如下：
     * 
     * <ol>
     * <li>
     * 路径为<code>null</code>，则返回<code>null</code>。
     * </li>
     * <li>
     * 将所有backslash("\\")转化成slash("/")。
     * </li>
     * <li>
     * 去除重复的"/"或"\\"。
     * </li>
     * <li>
     * 去除"."，如果发现".."，则向上朔一级目录。
     * </li>
     * <li>
     * 空路径返回"/"。
     * </li>
     * <li>
     * 保留路径末尾的"/"（如果有的话）。
     * </li>
     * <li>
     * 对于绝对路径，如果".."上朔的路径超过了根目录，则看作非法路径，返回<code>null</code>。
     * </li>
     * </ol>
     * </p>
     *
     * @param path 要规格化的路径
     *
     * @return 规格化后的路径，如果路径非法，则返回<code>null</code>
     */
    public static String normalizeAbsolutePath(String path) {
        String normalizedPath = normalizePath(path, false);

        if ((normalizedPath != null) && !normalizedPath.startsWith(SLASH)) {
            if (normalizedPath.equals(CURRENT_DIR)
                        || normalizedPath.equals(CURRENT_DIR + SLASH_CHAR)) {
                normalizedPath = SLASH;
            } else if (normalizedPath.startsWith(UP_LEVEL_DIR)) {
                normalizedPath = null;
            } else {
                normalizedPath = SLASH_CHAR + normalizedPath;
            }
        }

        return normalizedPath;
    }

    /**
     * 规格化路径。
     * 
     * <p>
     * 该方法自动判别操作系统的类型。转换规则如下：
     * 
     * <ol>
     * <li>
     * 路径为<code>null</code>，则返回<code>null</code>。
     * </li>
     * <li>
     * 将所有backslash("\\")转化成slash("/")。
     * </li>
     * <li>
     * 去除重复的"/"或"\\"。
     * </li>
     * <li>
     * 去除"."，如果发现".."，则向上朔一级目录。
     * </li>
     * <li>
     * 空绝对路径返回"/"，空相对路径返回"./"。
     * </li>
     * <li>
     * 保留路径末尾的"/"（如果有的话）。
     * </li>
     * <li>
     * 对于绝对路径，如果".."上朔的路径超过了根目录，则看作非法路径，返回<code>null</code>。
     * </li>
     * <li>
     * 对于Windows系统，有些路径有特殊的前缀，如驱动器名"c:"和UNC名"//hostname"，对于这些路径，保留其前缀，并对其后的路径部分适用上述所有规则。
     * </li>
     * <li>
     * Windows驱动器名被转换成大写，如"c:"转换成"C:"。
     * </li>
     * </ol>
     * </p>
     *
     * @param path 要规格化的路径
     *
     * @return 规格化后的路径，如果路径非法，则返回<code>null</code>
     */
    public static String normalizePath(String path) {
        return normalizePath(path, SystemUtil.getOsInfo().isWindows());
    }

    /**
     * 规格化路径。规则如下：
     * 
     * <ol>
     * <li>
     * 路径为<code>null</code>，则返回<code>null</code>。
     * </li>
     * <li>
     * 将所有backslash("\\")转化成slash("/")。
     * </li>
     * <li>
     * 去除重复的"/"或"\\"。
     * </li>
     * <li>
     * 去除"."，如果发现".."，则向上朔一级目录。
     * </li>
     * <li>
     * 空绝对路径返回"/"，空相对路径返回"./"。
     * </li>
     * <li>
     * 保留路径末尾的"/"（如果有的话）。
     * </li>
     * <li>
     * 对于绝对路径，如果".."上朔的路径超过了根目录，则看作非法路径，返回<code>null</code>。
     * </li>
     * <li>
     * 对于Windows系统，有些路径有特殊的前缀，如驱动器名"c:"和UNC名"//hostname"，对于这些路径，保留其前缀，并对其后的路径部分适用上述所有规则。
     * </li>
     * <li>
     * Windows驱动器名被转换成大写，如"c:"转换成"C:"。
     * </li>
     * </ol>
     * 
     *
     * @param path 要规格化的路径
     *
     * @return 规格化后的路径，如果路径非法，则返回<code>null</code>
     */
    public static String normalizeWindowsPath(String path) {
        return normalizePath(path, true);
    }

    /**
     * 规格化Unix风格的路径，不支持Windows驱动器名和UNC路径。
     * 
     * <p>
     * 转换规则如下：
     * 
     * <ol>
     * <li>
     * 路径为<code>null</code>，则返回<code>null</code>。
     * </li>
     * <li>
     * 将所有backslash("\\")转化成slash("/")。
     * </li>
     * <li>
     * 去除重复的"/"或"\\"。
     * </li>
     * <li>
     * 去除"."，如果发现".."，则向上朔一级目录。
     * </li>
     * <li>
     * 空绝对路径返回"/"，空相对路径返回"./"。
     * </li>
     * <li>
     * 保留路径末尾的"/"（如果有的话）。
     * </li>
     * <li>
     * 对于绝对路径，如果".."上朔的路径超过了根目录，则看作非法路径，返回<code>null</code>。
     * </li>
     * </ol>
     * </p>
     *
     * @param path 要规格化的路径
     *
     * @return 规格化后的路径，如果路径非法，则返回<code>null</code>
     */
    public static String normalizeUnixPath(String path) {
        return normalizePath(path, false);
    }

    /**
     * 规格化路径。规则如下：
     * 
     * <ol>
     * <li>
     * 路径为<code>null</code>，则返回<code>null</code>。
     * </li>
     * <li>
     * 将所有backslash("\\")转化成slash("/")。
     * </li>
     * <li>
     * 去除重复的"/"或"\\"。
     * </li>
     * <li>
     * 去除"."，如果发现".."，则向上朔一级目录。
     * </li>
     * <li>
     * 空绝对路径返回"/"，空相对路径返回"./"。
     * </li>
     * <li>
     * 保留路径末尾的"/"（如果有的话）。
     * </li>
     * <li>
     * 对于绝对路径，如果".."上朔的路径超过了根目录，则看作非法路径，返回<code>null</code>。
     * </li>
     * <li>
     * 对于Windows系统，有些路径有特殊的前缀，如驱动器名"c:"和UNC名"//hostname"，对于这些路径，保留其前缀，并对其后的路径部分适用上述所有规则。
     * </li>
     * <li>
     * Windows驱动器名被转换成大写，如"c:"转换成"C:"。
     * </li>
     * </ol>
     * 
     *
     * @param path 要规格化的路径
     * @param isWindows 是否是windows路径，如果为<code>true</code>，则支持驱动器名和UNC路径
     *
     * @return 规格化后的路径，如果路径非法，则返回<code>null</code>
     */
    private static String normalizePath(String path, boolean isWindows) {
        if (path == null) {
            return null;
        }

        path     = path.trim();

        // 将"\\"转换成"/"，以便统一处理
        path = path.replace(BACKSLASH_CHAR, SLASH_CHAR);

        // 取得系统特定的路径前缀，对于windows系统，可能是："C:"或是"//hostname"
        String prefix = getSystemDependentPrefix(path, isWindows);

        if (prefix == null) {
            return null;
        }

        path = path.substring(prefix.length());

        // 对于绝对路径，prefix必须以"/"结尾，反之，绝对路径的prefix.length > 0
        if ((prefix.length() > 0) || path.startsWith(SLASH)) {
            prefix += SLASH_CHAR;
        }

        // 保留path尾部的"/"
        boolean endsWithSlash = path.endsWith(SLASH);

        // 压缩路径中的"."和".."
        StringTokenizer tokenizer = new StringTokenizer(path, "/");
        StringBuffer    buffer = new StringBuffer(prefix.length() + path.length());
        int             level  = 0;

        buffer.append(prefix);

        while (tokenizer.hasMoreTokens()) {
            String element = tokenizer.nextToken();

            // 忽略"."
            if (CURRENT_DIR.equals(element)) {
                continue;
            }

            // 回朔".."
            if (UP_LEVEL_DIR.equals(element)) {
                if (level == 0) {
                    // 如果prefix存在，并且试图越过最上层目录，这是不可能的，
                    // 返回null，表示路径非法。
                    if (prefix.length() > 0) {
                        return null;
                    }

                    buffer.append(UP_LEVEL_DIR).append(SLASH_CHAR);
                } else {
                    level--;

                    boolean found = false;

                    for (int i = buffer.length() - 2; i >= prefix.length(); i--) {
                        if (buffer.charAt(i) == SLASH_CHAR) {
                            buffer.setLength(i + 1);
                            found = true;
                            break;
                        }
                    }

                    if (!found) {
                        buffer.setLength(prefix.length());
                    }
                }

                continue;
            }

            // 添加到path
            buffer.append(element).append(SLASH_CHAR);
            level++;
        }

        // 如果是空的路径，则设置为"./"
        if (buffer.length() == 0) {
            buffer.append(CURRENT_DIR).append(SLASH_CHAR);
        }

        // 保留最后的"/"
        if (!endsWithSlash && (buffer.length() > prefix.length())
                    && (buffer.charAt(buffer.length() - 1) == SLASH_CHAR)) {
            buffer.setLength(buffer.length() - 1);
        }

        return buffer.toString();
    }

    /**
     * 取得和系统相关的文件名前缀。对于Windows系统，可能是驱动器名或UNC路径前缀"//hostname"。如果不存在前缀，则返回空字符串。
     *
     * @param path 绝对路径
     * @param isWindows 是否为windows系统
     *
     * @return 和系统相关的文件名前缀，如果路径非法，例如："//"，则返回<code>null</code>
     */
    private static String getSystemDependentPrefix(String path, boolean isWindows) {
        if (isWindows) {
            // 判断UNC路径
            if (path.startsWith(UNC_PREFIX)) {
                // 非法UNC路径："//"
                if (path.length() == UNC_PREFIX.length()) {
                    return null;
                }

                // 假设路径为//hostname/subpath，返回//hostname
                int index = path.indexOf(SLASH, UNC_PREFIX.length());

                if (index != -1) {
                    return path.substring(0, index);
                } else {
                    return path;
                }
            }

            // 判断Windows绝对路径："c:/..."
            if ((path.length() > 1) && (path.charAt(1) == COLON_CHAR)) {
                return path.substring(0, 2).toUpperCase();
            }
        }

        return "";
    }

    /* ============================================================================ */
    /*  取得基于指定basedir规格化路径。                                             */
    /* ============================================================================ */

    /**
     * 如果指定路径已经是绝对路径，则规格化后直接返回之，否则取得基于指定basedir的规格化路径。
     * 
     * <p>
     * 该方法自动判定操作系统的类型，如果是windows系统，则支持UNC路径和驱动器名。
     * </p>
     *
     * @param basedir 根目录，如果<code>path</code>为相对路径，表示基于此目录
     * @param path 要检查的路径
     *
     * @return 规格化的路径，如果<code>path</code>非法，或<code>basedir</code>为<code>null</code>，则返回<code>null</code>
     */
    public static String getPathBasedOn(String basedir, String path) {
        return getPathBasedOn(basedir, path, SystemUtil.getOsInfo().isWindows());
    }

    /**
     * 如果指定路径已经是绝对路径，则规格化后直接返回之，否则取得基于指定basedir的规格化路径。
     *
     * @param basedir 根目录，如果<code>path</code>为相对路径，表示基于此目录
     * @param path 要检查的路径
     *
     * @return 规格化的路径，如果<code>path</code>非法，或<code>basedir</code>为<code>null</code>，则返回<code>null</code>
     */
    public static String getWindowsPathBasedOn(String basedir, String path) {
        return getPathBasedOn(basedir, path, true);
    }

    /**
     * 如果指定路径已经是绝对路径，则规格化后直接返回之，否则取得基于指定basedir的规格化路径。
     *
     * @param basedir 根目录，如果<code>path</code>为相对路径，表示基于此目录
     * @param path 要检查的路径
     *
     * @return 规格化的路径，如果<code>path</code>非法，或<code>basedir</code>为<code>null</code>，则返回<code>null</code>
     */
    public static String getUnixPathBasedOn(String basedir, String path) {
        return getPathBasedOn(basedir, path, false);
    }

    /**
     * 如果指定路径已经是绝对路径，则规格化后直接返回之，否则取得基于指定basedir的规格化路径。
     *
     * @param basedir 根目录，如果<code>path</code>为相对路径，表示基于此目录
     * @param path 要检查的路径
     * @param isWindows 是否是windows路径，如果为<code>true</code>，则支持驱动器名和UNC路径
     *
     * @return 规格化的路径，如果<code>path</code>非法，或<code>basedir</code>为<code>null</code>，则返回<code>null</code>
     */
    private static String getPathBasedOn(String basedir, String path, boolean isWindows) {
        /* ------------------------------------------- *
         * 首先取得path的前缀，判断是否为绝对路径。    *
         * 如果已经是绝对路径，则调用normalize后返回。 *
         * ------------------------------------------- */
        if (path == null) {
            return null;
        }

        path     = path.trim();

        // 将"\\"转换成"/"，以便统一处理
        path = path.replace(BACKSLASH_CHAR, SLASH_CHAR);

        // 取得系统特定的路径前缀，对于windows系统，可能是："C:"或是"//hostname"
        String prefix = getSystemDependentPrefix(path, isWindows);

        if (prefix == null) {
            return null;
        }

        // 如果是绝对路径，则直接返回
        if ((prefix.length() > 0)
                    || ((path.length() > prefix.length())
                    && (path.charAt(prefix.length()) == SLASH_CHAR))) {
            return normalizePath(path, isWindows);
        }

        /* ------------------------------------------- *
         * 现在已经确定path是相对路径了，因此我们要    *
         * 将它和basedir合并。                         *
         * ------------------------------------------- */
        if (basedir == null) {
            return null;
        }

        StringBuffer buffer = new StringBuffer();

        buffer.append(basedir.trim());

        // 防止重复的"/"，否则容易和UNC prefix混淆
        if ((basedir.length() > 0) && (path.length() > 0)
                    && (basedir.charAt(basedir.length() - 1) != SLASH_CHAR)) {
            buffer.append(SLASH_CHAR);
        }

        buffer.append(path);

        return normalizePath(buffer.toString(), isWindows);
    }

    /* ============================================================================ */
    /*  取得相对于指定basedir相对路径。                                             */
    /* ============================================================================ */

    /**
     * 取得相对于指定根目录的相对路径。
     * 
     * <p>
     * 该方法自动判定操作系统的类型，如果是windows系统，则支持UNC路径和驱动器名。
     * </p>
     *
     * @param basedir 根目录
     * @param path 要计算的路径
     *
     * @return 如果<code>path</code>和<code>basedir</code>是兼容的，则返回相对于<code>basedir</code>的相对路径，否则返回<code>path</code>本身。如果<code>basedir</code>不是绝对路径，或者路径非法，则返回<code>null</code>
     */
    public static String getRelativePath(String basedir, String path) {
        return getRelativePath(basedir, path, SystemUtil.getOsInfo().isWindows());
    }

    /**
     * 取得相对于指定根目录的相对路径。
     *
     * @param basedir 根目录
     * @param path 要计算的路径
     *
     * @return 如果<code>path</code>和<code>basedir</code>是兼容的，则返回相对于<code>basedir</code>的相对路径，否则返回<code>path</code>本身。如果<code>basedir</code>不是绝对路径，或者路径非法，则返回<code>null</code>
     */
    public static String getWindowsRelativePath(String basedir, String path) {
        return getRelativePath(basedir, path, true);
    }

    /**
     * 取得相对于指定根目录的相对路径。
     *
     * @param basedir 根目录
     * @param path 要计算的路径
     *
     * @return 如果<code>path</code>和<code>basedir</code>是兼容的，则返回相对于<code>basedir</code>的相对路径，否则返回<code>path</code>本身。如果<code>basedir</code>不是绝对路径，或者路径非法，则返回<code>null</code>
     */
    public static String getUnixRelativePath(String basedir, String path) {
        return getRelativePath(basedir, path, false);
    }

    /**
     * 取得相对于指定根目录的相对路径。
     *
     * @param basedir 根目录
     * @param path 要计算的路径
     * @param isWindows 是否是windows路径，如果为<code>true</code>，则支持驱动器名和UNC路径
     *
     * @return 如果<code>path</code>和<code>basedir</code>是兼容的，则返回相对于<code>basedir</code>的相对路径，否则返回<code>path</code>本身。如果<code>basedir</code>不是绝对路径，或者路径非法，则返回<code>null</code>
     */
    private static String getRelativePath(String basedir, String path, boolean isWindows) {
        // 取得规格化的basedir，确保其为绝对路径
        basedir = normalizePath(basedir, isWindows);

        if (basedir == null) {
            return null;
        }

        String basePrefix = getSystemDependentPrefix(basedir, isWindows);

        if ((basePrefix == null) || ((basePrefix.length() == 0) && !basedir.startsWith(SLASH))) {
            return null; // basedir必须是绝对路径
        }

        // 取得规格化的path
        path = getPathBasedOn(basedir, path, isWindows);

        if (path == null) {
            return null;
        }

        String prefix = getSystemDependentPrefix(path, isWindows);

        // 如果path和basedir的前缀不同，则不能转换成相对于basedir的相对路径。
        // 直接返回规格化的path即可。
        if (!basePrefix.equals(prefix)) {
            return path;
        }

        // 保留path尾部的"/"
        boolean endsWithSlash = path.endsWith(SLASH);

        // 按"/"分隔basedir和path
        String[]     baseParts = StringUtil.split(basedir.substring(basePrefix.length()), SLASH_CHAR);
        String[]     parts  = StringUtil.split(path.substring(prefix.length()), SLASH_CHAR);
        StringBuffer buffer = new StringBuffer();
        int          i      = 0;

        if (isWindows) {
            while ((i < baseParts.length) && (i < parts.length)
                        && baseParts[i].equalsIgnoreCase(parts[i])) {
                i++;
            }
        } else {
            while ((i < baseParts.length) && (i < parts.length) && baseParts[i].equals(parts[i])) {
                i++;
            }
        }

        if ((i < baseParts.length) && (i < parts.length)) {
            for (int j = i; j < baseParts.length; j++) {
                buffer.append(UP_LEVEL_DIR).append(SLASH_CHAR);
            }
        }

        for (; i < parts.length; i++) {
            buffer.append(parts[i]);

            if (i < (parts.length - 1)) {
                buffer.append(SLASH_CHAR);
            }
        }

        if (buffer.length() == 0) {
            buffer.append(CURRENT_DIR);
        }

        String relpath = buffer.toString();

        if (endsWithSlash && !relpath.endsWith(SLASH)) {
            relpath += SLASH;
        }

        return relpath;
    }

    /**
     * 从URL中取得文件。
     *
     * @param url URL
     *
     * @return 文件, 如果URL为空，或不代表一个文件, 则返回<code>null</code>
     */
    public static File toFile(URL url) {
        if (url == null) {
            return null;
        }

        if (url.getProtocol().equals("file")) {
            String path = url.getPath();

            if (path != null) {
                return new File(StringEscapeUtil.unescapeURL(path));
            }
        }

        return null;
    }

    /**
     * 取得指定路径的名称和后缀。 返回值为长度为2的数组：
     * 
     * <ol>
     * <li>
     * 第一个元素为不包含后缀的路径，总不为<code>null</code>。
     * </li>
     * <li>
     * 第二个元素为后缀，如果后缀不存在，则为<code>null</code>。
     * </li>
     * </ol>
     * 
     *
     * @param path 路径
     *
     * @return 路径和后缀的数组
     */
    public static String[] parseExtension(String path) {
        path = StringUtil.trimToEmpty(path);

        String[] parts = { path, null };

        if (StringUtil.isEmpty(path)) {
            return parts;
        }

        // 如果找到后缀，则index >= 0，且extension != null（除非name以.结尾）
        int    index     = StringUtil.lastIndexOf(path, EXTENSION_SEPARATOR);
        String extension = null;

        if (index >= 0) {
            extension = StringUtil.trimToNull(StringUtil.substring(path, index + 1));

            if (!StringUtil.containsNone(extension, ALL_SLASH)) {
                extension     = null;
                index         = -1;
            }
        }

        if (index >= 0) {
            parts[0] = StringUtil.substring(path, 0, index);
        }

        parts[1] = extension;

        return parts;
    }
}
