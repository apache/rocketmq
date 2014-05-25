package com.alibaba.common.lang.internal;

import java.util.HashMap;
import java.util.Map;

/**
 * 代表HTML和XML的实体定义。
 * 
 * <p>
 * 该类采用一个hash表和lookup查找表相结合的算法，确保搜索entities的性能。
 * </p>
 *
 * @author Michael Zhou
 * @version $Id: Entities.java 1132 2004-08-03 00:55:09Z baobao $
 *
 * @see <a href="http://hotwired.lycos.com/webmonkey/reference/special_characters/">ISO
 *      Entities</a>
 * @see <a href="http://www.w3.org/TR/REC-html32#latin1">HTML 3.2 Character Entities for ISO
 *      Latin-1</a>
 * @see <a href="http://www.w3.org/TR/REC-html40/sgml/entities.html">HTML 4.0 Character entity
 *      references</a>
 * @see <a href="http://www.w3.org/TR/html401/charset.html#h-5.3">HTML 4.01 Character
 *      References</a>
 * @see <a href="http://www.w3.org/TR/html401/charset.html#code-position">HTML 4.01 Code
 *      positions</a>
 */
public class Entities {
    /* ============================================================================ */
    /*  常量和singleton。                                                           */
    /* ============================================================================ */

    /** 基本实体定义。 */
    private static final String[][] BASIC_SET = {
            { "quot", "34" }, // " - double-quote
            { "amp", "38" }, // & - ampersand
            { "lt", "60" }, // < - less-than
            { "gt", "62" }, // > - greater-than
        };

    /** XML省略语。 */
    private static final String[][] APOS_SET = {
            { "apos", "39" }, // XML apostrophe
        };

    /** 8859-1实体定义。 */
    private static final String[][] ISO_8859_1_SET = {
            { "nbsp", "160" }, // non-breaking space
            { "iexcl", "161" }, // inverted exclamation mark
            { "cent", "162" }, // cent sign
            { "pound", "163" }, // pound sign
            { "curren", "164" }, // currency sign
            { "yen", "165" }, // yen sign = yuan sign
            { "brvbar", "166" }, // broken bar = broken vertical bar
            { "sect", "167" }, // section sign
            { "uml", "168" }, // diaeresis = spacing diaeresis
            { "copy", "169" }, // ? - copyright sign
            { "ordf", "170" }, // feminine ordinal indicator
            { "laquo", "171" }, // left-pointing double angle quotation mark = left pointing guillemet
            { "not", "172" }, // not sign
            { "shy", "173" }, // soft hyphen = discretionary hyphen
            { "reg", "174" }, // ? - registered trademark sign
            { "macr", "175" }, // macron = spacing macron = overline = APL overbar
            { "deg", "176" }, // degree sign
            { "plusmn", "177" }, // plus-minus sign = plus-or-minus sign
            { "sup2", "178" }, // superscript two = superscript digit two = squared
            { "sup3", "179" }, // superscript three = superscript digit three = cubed
            { "acute", "180" }, // acute accent = spacing acute
            { "micro", "181" }, // micro sign
            { "para", "182" }, // pilcrow sign = paragraph sign
            { "middot", "183" }, // middle dot = Georgian comma = Greek middle dot
            { "cedil", "184" }, // cedilla = spacing cedilla
            { "sup1", "185" }, // superscript one = superscript digit one
            { "ordm", "186" }, // masculine ordinal indicator
            { "raquo", "187" }, // right-pointing double angle quotation mark = right pointing guillemet
            { "frac14", "188" }, // vulgar fraction one quarter = fraction one quarter
            { "frac12", "189" }, // vulgar fraction one half = fraction one half
            { "frac34", "190" }, // vulgar fraction three quarters = fraction three quarters
            { "iquest", "191" }, // inverted question mark = turned question mark
            { "Agrave", "192" }, // ? - uppercase A, grave accent
            { "Aacute", "193" }, // ? - uppercase A, acute accent
            { "Acirc", "194" }, // ? - uppercase A, circumflex accent
            { "Atilde", "195" }, // ? - uppercase A, tilde
            { "Auml", "196" }, // ? - uppercase A, umlaut
            { "Aring", "197" }, // ? - uppercase A, ring
            { "AElig", "198" }, // ? - uppercase AE
            { "Ccedil", "199" }, // ? - uppercase C, cedilla
            { "Egrave", "200" }, // ? - uppercase E, grave accent
            { "Eacute", "201" }, // ? - uppercase E, acute accent
            { "Ecirc", "202" }, // ? - uppercase E, circumflex accent
            { "Euml", "203" }, // ? - uppercase E, umlaut
            { "Igrave", "204" }, // ? - uppercase I, grave accent
            { "Iacute", "205" }, // ? - uppercase I, acute accent
            { "Icirc", "206" }, // ? - uppercase I, circumflex accent
            { "Iuml", "207" }, // ? - uppercase I, umlaut
            { "ETH", "208" }, // ? - uppercase Eth, Icelandic
            { "Ntilde", "209" }, // ? - uppercase N, tilde
            { "Ograve", "210" }, // ? - uppercase O, grave accent
            { "Oacute", "211" }, // ? - uppercase O, acute accent
            { "Ocirc", "212" }, // ? - uppercase O, circumflex accent
            { "Otilde", "213" }, // ? - uppercase O, tilde
            { "Ouml", "214" }, // ? - uppercase O, umlaut
            { "times", "215" }, // multiplication sign
            { "Oslash", "216" }, // ? - uppercase O, slash
            { "Ugrave", "217" }, // ? - uppercase U, grave accent
            { "Uacute", "218" }, // ? - uppercase U, acute accent
            { "Ucirc", "219" }, // ? - uppercase U, circumflex accent
            { "Uuml", "220" }, // ? - uppercase U, umlaut
            { "Yacute", "221" }, // ? - uppercase Y, acute accent
            { "THORN", "222" }, // ? - uppercase THORN, Icelandic
            { "szlig", "223" }, // ? - lowercase sharps, German
            { "agrave", "224" }, // ? - lowercase a, grave accent
            { "aacute", "225" }, // ? - lowercase a, acute accent
            { "acirc", "226" }, // ? - lowercase a, circumflex accent
            { "atilde", "227" }, // ? - lowercase a, tilde
            { "auml", "228" }, // ? - lowercase a, umlaut
            { "aring", "229" }, // ? - lowercase a, ring
            { "aelig", "230" }, // ? - lowercase ae
            { "ccedil", "231" }, // ? - lowercase c, cedilla
            { "egrave", "232" }, // ? - lowercase e, grave accent
            { "eacute", "233" }, // ? - lowercase e, acute accent
            { "ecirc", "234" }, // ? - lowercase e, circumflex accent
            { "euml", "235" }, // ? - lowercase e, umlaut
            { "igrave", "236" }, // ? - lowercase i, grave accent
            { "iacute", "237" }, // ? - lowercase i, acute accent
            { "icirc", "238" }, // ? - lowercase i, circumflex accent
            { "iuml", "239" }, // ? - lowercase i, umlaut
            { "eth", "240" }, // ? - lowercase eth, Icelandic
            { "ntilde", "241" }, // ? - lowercase n, tilde
            { "ograve", "242" }, // ? - lowercase o, grave accent
            { "oacute", "243" }, // ? - lowercase o, acute accent
            { "ocirc", "244" }, // ? - lowercase o, circumflex accent
            { "otilde", "245" }, // ? - lowercase o, tilde
            { "ouml", "246" }, // ? - lowercase o, umlaut
            { "divide", "247" }, // division sign
            { "oslash", "248" }, // ? - lowercase o, slash
            { "ugrave", "249" }, // ? - lowercase u, grave accent
            { "uacute", "250" }, // ? - lowercase u, acute accent
            { "ucirc", "251" }, // ? - lowercase u, circumflex accent
            { "uuml", "252" }, // ? - lowercase u, umlaut
            { "yacute", "253" }, // ? - lowercase y, acute accent
            { "thorn", "254" }, // ? - lowercase thorn, Icelandic
            { "yuml", "255" }, // ? - lowercase y, umlaut
        };

    /** HTML 4.0实体定义（http://www.w3.org/TR/REC-html40/sgml/entities.html）。 */
    private static final String[][] HTML40_SET = {
            { "fnof", "402" }, // latin small f with hook = function= florin, U+0192 ISOtech 
            { "Alpha", "913" }, // greek capital letter alpha, U+0391 
            { "Beta", "914" }, // greek capital letter beta, U+0392 
            { "Gamma", "915" }, // greek capital letter gamma,U+0393 ISOgrk3 
            { "Delta", "916" }, // greek capital letter delta,U+0394 ISOgrk3 
            { "Epsilon", "917" }, // greek capital letter epsilon, U+0395 
            { "Zeta", "918" }, // greek capital letter zeta, U+0396 
            { "Eta", "919" }, // greek capital letter eta, U+0397 
            { "Theta", "920" }, // greek capital letter theta,U+0398 ISOgrk3 
            { "Iota", "921" }, // greek capital letter iota, U+0399 
            { "Kappa", "922" }, // greek capital letter kappa, U+039A 
            { "Lambda", "923" }, // greek capital letter lambda,U+039B ISOgrk3 
            { "Mu", "924" }, // greek capital letter mu, U+039C 
            { "Nu", "925" }, // greek capital letter nu, U+039D 
            { "Xi", "926" }, // greek capital letter xi, U+039E ISOgrk3 
            { "Omicron", "927" }, // greek capital letter omicron, U+039F 
            { "Pi", "928" }, // greek capital letter pi, U+03A0 ISOgrk3 
            { "Rho", "929" }, // greek capital letter rho, U+03A1 
            { "Sigma", "931" }, // greek capital letter sigma,U+03A3 ISOgrk3 
            { "Tau", "932" }, // greek capital letter tau, U+03A4 
            { "Upsilon", "933" }, // greek capital letter upsilon,U+03A5 ISOgrk3 
            { "Phi", "934" }, // greek capital letter phi,U+03A6 ISOgrk3 
            { "Chi", "935" }, // greek capital letter chi, U+03A7 
            { "Psi", "936" }, // greek capital letter psi,U+03A8 ISOgrk3 
            { "Omega", "937" }, // greek capital letter omega,U+03A9 ISOgrk3 
            { "alpha", "945" }, // greek small letter alpha,U+03B1 ISOgrk3 
            { "beta", "946" }, // greek small letter beta, U+03B2 ISOgrk3 
            { "gamma", "947" }, // greek small letter gamma,U+03B3 ISOgrk3 
            { "delta", "948" }, // greek small letter delta,U+03B4 ISOgrk3 
            { "epsilon", "949" }, // greek small letter epsilon,U+03B5 ISOgrk3 
            { "zeta", "950" }, // greek small letter zeta, U+03B6 ISOgrk3 
            { "eta", "951" }, // greek small letter eta, U+03B7 ISOgrk3 
            { "theta", "952" }, // greek small letter theta,U+03B8 ISOgrk3 
            { "iota", "953" }, // greek small letter iota, U+03B9 ISOgrk3 
            { "kappa", "954" }, // greek small letter kappa,U+03BA ISOgrk3 
            { "lambda", "955" }, // greek small letter lambda,U+03BB ISOgrk3 
            { "mu", "956" }, // greek small letter mu, U+03BC ISOgrk3 
            { "nu", "957" }, // greek small letter nu, U+03BD ISOgrk3 
            { "xi", "958" }, // greek small letter xi, U+03BE ISOgrk3 
            { "omicron", "959" }, // greek small letter omicron, U+03BF NEW 
            { "pi", "960" }, // greek small letter pi, U+03C0 ISOgrk3 
            { "rho", "961" }, // greek small letter rho, U+03C1 ISOgrk3 
            { "sigmaf", "962" }, // greek small letter final sigma,U+03C2 ISOgrk3 
            { "sigma", "963" }, // greek small letter sigma,U+03C3 ISOgrk3 
            { "tau", "964" }, // greek small letter tau, U+03C4 ISOgrk3 
            { "upsilon", "965" }, // greek small letter upsilon,U+03C5 ISOgrk3 
            { "phi", "966" }, // greek small letter phi, U+03C6 ISOgrk3 
            { "chi", "967" }, // greek small letter chi, U+03C7 ISOgrk3 
            { "psi", "968" }, // greek small letter psi, U+03C8 ISOgrk3 
            { "omega", "969" }, // greek small letter omega,U+03C9 ISOgrk3 
            { "thetasym", "977" }, // greek small letter theta symbol,U+03D1 NEW 
            { "upsih", "978" }, // greek upsilon with hook symbol,U+03D2 NEW 
            { "piv", "982" }, // greek pi symbol, U+03D6 ISOgrk3 
            { "bull", "8226" }, // bullet = black small circle,U+2022 ISOpub  
            { "hellip", "8230" }, // horizontal ellipsis = three dot leader,U+2026 ISOpub  
            { "prime", "8242" }, // prime = minutes = feet, U+2032 ISOtech 
            { "Prime", "8243" }, // double prime = seconds = inches,U+2033 ISOtech 
            { "oline", "8254" }, // overline = spacing overscore,U+203E NEW 
            { "frasl", "8260" }, // fraction slash, U+2044 NEW 
            { "weierp", "8472" }, // script capital P = power set= Weierstrass p, U+2118 ISOamso 
            { "image", "8465" }, // blackletter capital I = imaginary part,U+2111 ISOamso 
            { "real", "8476" }, // blackletter capital R = real part symbol,U+211C ISOamso 
            { "trade", "8482" }, // trade mark sign, U+2122 ISOnum 
            { "alefsym", "8501" }, // alef symbol = first transfinite cardinal,U+2135 NEW 
            { "larr", "8592" }, // leftwards arrow, U+2190 ISOnum 
            { "uarr", "8593" }, // upwards arrow, U+2191 ISOnum
            { "rarr", "8594" }, // rightwards arrow, U+2192 ISOnum 
            { "darr", "8595" }, // downwards arrow, U+2193 ISOnum 
            { "harr", "8596" }, // left right arrow, U+2194 ISOamsa 
            { "crarr", "8629" }, // downwards arrow with corner leftwards= carriage return, U+21B5 NEW 
            { "lArr", "8656" }, // leftwards double arrow, U+21D0 ISOtech 
            { "uArr", "8657" }, // upwards double arrow, U+21D1 ISOamsa 
            { "rArr", "8658" }, // rightwards double arrow,U+21D2 ISOtech 
            { "dArr", "8659" }, // downwards double arrow, U+21D3 ISOamsa 
            { "hArr", "8660" }, // left right double arrow,U+21D4 ISOamsa 
            { "forall", "8704" }, // for all, U+2200 ISOtech 
            { "part", "8706" }, // partial differential, U+2202 ISOtech  
            { "exist", "8707" }, // there exists, U+2203 ISOtech 
            { "empty", "8709" }, // empty set = null set = diameter,U+2205 ISOamso 
            { "nabla", "8711" }, // nabla = backward difference,U+2207 ISOtech 
            { "isin", "8712" }, // element of, U+2208 ISOtech 
            { "notin", "8713" }, // not an element of, U+2209 ISOtech 
            { "ni", "8715" }, // contains as member, U+220B ISOtech 
            { "prod", "8719" }, // n-ary product = product sign,U+220F ISOamsb 
            { "sum", "8721" }, // n-ary sumation, U+2211 ISOamsb 
            { "minus", "8722" }, // minus sign, U+2212 ISOtech 
            { "lowast", "8727" }, // asterisk operator, U+2217 ISOtech 
            { "radic", "8730" }, // square root = radical sign,U+221A ISOtech 
            { "prop", "8733" }, // proportional to, U+221D ISOtech 
            { "infin", "8734" }, // infinity, U+221E ISOtech 
            { "ang", "8736" }, // angle, U+2220 ISOamso 
            { "and", "8743" }, // logical and = wedge, U+2227 ISOtech 
            { "or", "8744" }, // logical or = vee, U+2228 ISOtech 
            { "cap", "8745" }, // intersection = cap, U+2229 ISOtech 
            { "cup", "8746" }, // union = cup, U+222A ISOtech 
            { "int", "8747" }, // integral, U+222B ISOtech 
            { "there4", "8756" }, // therefore, U+2234 ISOtech 
            { "sim", "8764" }, // tilde operator = varies with = similar to,U+223C ISOtech 
            { "cong", "8773" }, // approximately equal to, U+2245 ISOtech 
            { "asymp", "8776" }, // almost equal to = asymptotic to,U+2248 ISOamsr 
            { "ne", "8800" }, // not equal to, U+2260 ISOtech 
            { "equiv", "8801" }, // identical to, U+2261 ISOtech 
            { "le", "8804" }, // less-than or equal to, U+2264 ISOtech 
            { "ge", "8805" }, // greater-than or equal to,U+2265 ISOtech 
            { "sub", "8834" }, // subset of, U+2282 ISOtech 
            { "sup", "8835" }, // superset of, U+2283 ISOtech 
            { "sube", "8838" }, // subset of or equal to, U+2286 ISOtech 
            { "supe", "8839" }, // superset of or equal to,U+2287 ISOtech 
            { "oplus", "8853" }, // circled plus = direct sum,U+2295 ISOamsb 
            { "otimes", "8855" }, // circled times = vector product,U+2297 ISOamsb 
            { "perp", "8869" }, // up tack = orthogonal to = perpendicular,U+22A5 ISOtech 
            { "sdot", "8901" }, // dot operator, U+22C5 ISOamsb 
            { "lceil", "8968" }, // left ceiling = apl upstile,U+2308 ISOamsc  
            { "rceil", "8969" }, // right ceiling, U+2309 ISOamsc  
            { "lfloor", "8970" }, // left floor = apl downstile,U+230A ISOamsc  
            { "rfloor", "8971" }, // right floor, U+230B ISOamsc  
            { "lang", "9001" }, // left-pointing angle bracket = bra,U+2329 ISOtech 
            { "rang", "9002" }, // right-pointing angle bracket = ket,U+232A ISOtech 
            { "loz", "9674" }, // lozenge, U+25CA ISOpub 
            { "spades", "9824" }, // black spade suit, U+2660 ISOpub 
            { "clubs", "9827" }, // black club suit = shamrock,U+2663 ISOpub 
            { "hearts", "9829" }, // black heart suit = valentine,U+2665 ISOpub 
            { "diams", "9830" }, // black diamond suit, U+2666 ISOpub 
            { "OElig", "338" }, // latin capital ligature OE,U+0152 ISOlat2 
            { "oelig", "339" }, // latin small ligature oe, U+0153 ISOlat2 
            { "Scaron", "352" }, // latin capital letter S with caron,U+0160 ISOlat2 
            { "scaron", "353" }, // latin small letter s with caron,U+0161 ISOlat2 
            { "Yuml", "376" }, // latin capital letter Y with diaeresis,U+0178 ISOlat2 
            { "circ", "710" }, // modifier letter circumflex accent,U+02C6 ISOpub 
            { "tilde", "732" }, // small tilde, U+02DC ISOdia 
            { "ensp", "8194" }, // en space, U+2002 ISOpub 
            { "emsp", "8195" }, // em space, U+2003 ISOpub 
            { "thinsp", "8201" }, // thin space, U+2009 ISOpub 
            { "zwnj", "8204" }, // zero width non-joiner,U+200C NEW RFC 2070 
            { "zwj", "8205" }, // zero width joiner, U+200D NEW RFC 2070 
            { "lrm", "8206" }, // left-to-right mark, U+200E NEW RFC 2070 
            { "rlm", "8207" }, // right-to-left mark, U+200F NEW RFC 2070 
            { "ndash", "8211" }, // en dash, U+2013 ISOpub 
            { "mdash", "8212" }, // em dash, U+2014 ISOpub 
            { "lsquo", "8216" }, // left single quotation mark,U+2018 ISOnum 
            { "rsquo", "8217" }, // right single quotation mark,U+2019 ISOnum 
            { "sbquo", "8218" }, // single low-9 quotation mark, U+201A NEW 
            { "ldquo", "8220" }, // left double quotation mark,U+201C ISOnum 
            { "rdquo", "8221" }, // right double quotation mark,U+201D ISOnum 
            { "bdquo", "8222" }, // double low-9 quotation mark, U+201E NEW 
            { "dagger", "8224" }, // dagger, U+2020 ISOpub 
            { "Dagger", "8225" }, // double dagger, U+2021 ISOpub 
            { "permil", "8240" }, // per mille sign, U+2030 ISOtech 
            { "lsaquo", "8249" }, // single left-pointing angle quotation mark,U+2039 ISO proposed 
            { "rsaquo", "8250" }, // single right-pointing angle quotation mark,U+203A ISO proposed 
            { "euro", "8364" }, // euro sign, U+20AC NEW 
        };

    /** XML规范定义的实体集。 */
    public static final Entities XML;

    /** HTML 3.2规范定义的实体集。 */
    public static final Entities HTML32;

    /** HTML 4.0规范定义的实体集。 */
    public static final Entities HTML40;

    /**
     * 初始化预定义的实体集。
     */
    static {
        XML = new Entities();
        XML.addEntities(BASIC_SET);
        XML.addEntities(APOS_SET);

        HTML32 = new Entities();
        HTML32.addEntities(BASIC_SET);
        HTML32.addEntities(ISO_8859_1_SET);

        HTML40 = new Entities();
        HTML40.addEntities(BASIC_SET);
        HTML40.addEntities(ISO_8859_1_SET);
        HTML40.addEntities(HTML40_SET);
    }

    /** 快速查找表的大小。 */
    private static final int LOOKUP_TABLE_SIZE = 256;

    /* ============================================================================ */
    /*  Entities对象成员变量。                                                      */
    /* ============================================================================ */
    private Map        entityNameToValue = new HashMap();
    private IntHashMap entityValueToName = new IntHashMap();
    private String[]   lookupTable;

    /**
     * 添加一组entities。
     *
     * @param entitySet entity定义数组
     */
    public void addEntities(String[][] entitySet) {
        for (int i = 0; i < entitySet.length; ++i) {
            addEntity(entitySet[i][0], Integer.parseInt(entitySet[i][1]));
        }
    }

    /**
     * 添加一个entity。
     *
     * @param name entity名称
     * @param value entity值
     */
    public void addEntity(String name, int value) {
        entityNameToValue.put(name, new Integer(value));
        entityValueToName.put(value, name);

        // 更新查找表
        if ((value < LOOKUP_TABLE_SIZE) && (lookupTable != null)) {
            lookupTable[value] = name;
        }
    }

    /**
     * 取得指定值对应的entity名称。
     *
     * @param value entity的值
     *
     * @return entity的名称
     */
    public String getEntityName(int value) {
        if (value < LOOKUP_TABLE_SIZE) {
            // 创建快速查找表
            if (lookupTable == null) {
                lookupTable = new String[LOOKUP_TABLE_SIZE];

                for (int i = 0; i < LOOKUP_TABLE_SIZE; ++i) {
                    lookupTable[i] = (String) entityValueToName.get(i);
                }
            }

            return lookupTable[value];
        }

        return (String) entityValueToName.get(value);
    }

    /**
     * 取得指定entity名称对应的entity值。
     *
     * @param name entity的名称
     *
     * @return entity的值
     */
    public int getEntityValue(String name) {
        Object value = entityNameToValue.get(name);

        if (value == null) {
            return -1;
        }

        return ((Integer) value).intValue();
    }
}
