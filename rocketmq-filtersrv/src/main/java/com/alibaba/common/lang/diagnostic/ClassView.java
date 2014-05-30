package com.alibaba.common.lang.diagnostic;

import com.alibaba.common.lang.ClassLoaderUtil;
import com.alibaba.common.lang.ClassUtil;
import com.alibaba.common.lang.ObjectUtil;

/**
 * 显示指定类的类层次结构以及装入该类的classloader信息。
 * 
 * <p>
 * 该工具可在命令行上直接执行。
 * </p>
 *
 * @author Michael Zhou
 * @version $Id: ClassView.java 509 2004-02-16 05:42:07Z baobao $
 */
public class ClassView {
    public static String toString(Class clazz) {
        if (clazz.isPrimitive()) {
            return ClassUtil.getClassName(clazz);
        } else if (clazz.isArray()) {
            return "Array " + ClassUtil.getClassName(clazz) + "\n"
            + toString(ClassUtil.getArrayComponentType(clazz));
        } else if (clazz.isInterface()) {
            return toInterfaceString(clazz, "");
        } else {
            return toClassString(clazz, "");
        }
    }

    private static String toInterfaceString(Class clazz, String indent) {
        StringBuffer buffer = new StringBuffer();

        buffer.append(indent).append("Interface ").append(clazz.getName()).append("  (")
              .append(toClassString(clazz)).append(')');

        Class[] interfaceClass = clazz.getInterfaces();

        for (int i = 0, c = interfaceClass.length; i < c; ++i) {
            clazz = interfaceClass[i];

            buffer.append('\n').append(toInterfaceString(clazz, indent + "  "));
        }

        return buffer.toString();
    }

    private static String toClassString(Class clazz, String indent) {
        StringBuffer buffer = new StringBuffer();

        buffer.append(indent).append("Class ").append(clazz.getName()).append("  (")
              .append(toClassString(clazz)).append(')');

        indent += "  ";

        Class[] interfaceClass = clazz.getInterfaces();

        for (int i = 0, c = interfaceClass.length; i < c; ++i) {
            buffer.append('\n').append(toInterfaceString(interfaceClass[i], indent));
        }

        clazz = clazz.getSuperclass();

        if (clazz != null) {
            buffer.append('\n').append(toClassString(clazz, indent));
        }

        return buffer.toString();
    }

    private static String toClassString(Class clz) {
        ClassLoader loader = clz.getClassLoader();

        return "loaded by " + ObjectUtil.identityToString(loader, "System ClassLoader") + ", "
        + ClassLoaderUtil.whichClass(clz.getName());
    }

    public static void main(String[] args) throws ClassNotFoundException {
        if (args.length == 0) {
            System.out.println("\nUsage:");
            System.out.println("    java " + ClassView.class.getName() + " MyClass");
            System.out.println("    java " + ClassView.class.getName() + " my.package.MyClass");
            System.out.println("    java " + ClassView.class.getName() + " META-INF/MANIFEST.MF");
            System.exit(-1);
        }

        for (int i = 0; i < args.length; i++) {
            System.out.println(toString(ClassLoaderUtil.loadClass(args[i])));
        }
    }
}
