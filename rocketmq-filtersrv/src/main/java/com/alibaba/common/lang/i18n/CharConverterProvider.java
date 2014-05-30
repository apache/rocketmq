package com.alibaba.common.lang.i18n;

/**
 * 代表一个<code>CharConverter</code>方案的提供者。
 *
 * @author Michael Zhou
 */
public interface CharConverterProvider {
    /**
     * 创建一个新的converter。
     */
    CharConverter createCharConverter();
}
