package com.alibaba.common.lang;

import java.math.BigInteger;

/**
 * 包装<code>java.lang.Math</code>的工具类。
 *
 * @author Michael Zhou
 * @version $Id: MathUtil.java 1163 2004-08-20 02:56:58Z baobao $
 */
public class MathUtil {
    /**
     * The <code>double</code> value that is closer than any other to <i>e</i>, the base of the
     * natural logarithms.
     */
    public static double getE() {
        return Math.E;
    }

    /**
     * The <code>double</code> value that is closer than any other to <i>pi</i>, the ratio of the
     * circumference of a circle to its diameter.
     */
    public static double getPI() {
        return Math.PI;
    }

    /**
     * Returns the trigonometric sine of an angle.  Special cases:
     * 
     * <ul>
     * <li>
     * If the argument is NaN or an infinity, then the  result is NaN.
     * </li>
     * <li>
     * If the argument is zero, then the result is a zero with the same sign as the argument.
     * </li>
     * </ul>
     * 
     * <p>
     * A result must be within 1 ulp of the correctly rounded result.  Results must be
     * semi-monotonic.
     * </p>
     *
     * @param a an angle, in radians.
     *
     * @return the sine of the argument.
     */
    public static double sin(double a) {
        return Math.sin(a);
    }

    /**
     * Returns the trigonometric cosine of an angle. Special cases:
     * 
     * <ul>
     * <li>
     * If the argument is NaN or an infinity, then the  result is NaN.
     * </li>
     * </ul>
     * 
     * <p>
     * A result must be within 1 ulp of the correctly rounded result.  Results must be
     * semi-monotonic.
     * </p>
     *
     * @param a an angle, in radians.
     *
     * @return the cosine of the argument.
     */
    public static double cos(double a) {
        return Math.cos(a);
    }

    /**
     * Returns the trigonometric tangent of an angle.  Special cases:
     * 
     * <ul>
     * <li>
     * If the argument is NaN or an infinity, then the result  is NaN.
     * </li>
     * <li>
     * If the argument is zero, then the result is a zero with the same sign as the argument.
     * </li>
     * </ul>
     * 
     * <p>
     * A result must be within 1 ulp of the correctly rounded result.  Results must be
     * semi-monotonic.
     * </p>
     *
     * @param a an angle, in radians.
     *
     * @return the tangent of the argument.
     */
    public static double tan(double a) {
        return Math.tan(a);
    }

    /**
     * Returns the arc sine of an angle, in the range of -<i>pi</i>/2 through <i>pi</i>/2. Special
     * cases:
     * 
     * <ul>
     * <li>
     * If the argument is NaN or its absolute value is greater  than 1, then the result is NaN.
     * </li>
     * <li>
     * If the argument is zero, then the result is a zero with the same sign as the argument.
     * </li>
     * </ul>
     * 
     * <p>
     * A result must be within 1 ulp of the correctly rounded result.  Results must be
     * semi-monotonic.
     * </p>
     *
     * @param a the value whose arc sine is to be returned.
     *
     * @return the arc sine of the argument.
     */
    public static double asin(double a) {
        return Math.asin(a);
    }

    /**
     * Returns the arc cosine of an angle, in the range of 0.0 through <i>pi</i>.  Special case:
     * 
     * <ul>
     * <li>
     * If the argument is NaN or its absolute value is greater  than 1, then the result is NaN.
     * </li>
     * </ul>
     * 
     * <p>
     * A result must be within 1 ulp of the correctly rounded result.  Results  must be
     * semi-monotonic.
     * </p>
     *
     * @param a the value whose arc cosine is to be returned.
     *
     * @return the arc cosine of the argument.
     */
    public static double acos(double a) {
        return Math.acos(a);
    }

    /**
     * Returns the arc tangent of an angle, in the range of -<i>pi</i>/2 through <i>pi</i>/2.
     * Special cases:
     * 
     * <ul>
     * <li>
     * If the argument is NaN, then the result is NaN.
     * </li>
     * <li>
     * If the argument is zero, then the result is a zero with the same sign as the argument.
     * </li>
     * </ul>
     * 
     * <p>
     * A result must be within 1 ulp of the correctly rounded result.  Results must be
     * semi-monotonic.
     * </p>
     *
     * @param a the value whose arc tangent is to be returned.
     *
     * @return the arc tangent of the argument.
     */
    public static double atan(double a) {
        return Math.atan(a);
    }

    /**
     * Converts an angle measured in degrees to an approximately equivalent angle measured in
     * radians.  The conversion from degrees to radians is generally inexact.
     *
     * @param angdeg an angle, in degrees
     *
     * @return the measurement of the angle <code>angdeg</code> in radians.
     *
     * @since 1.2
     */
    public static double toRadians(double angdeg) {
        return Math.toRadians(angdeg);
    }

    /**
     * Converts an angle measured in radians to an approximately equivalent angle measured in
     * degrees.  The conversion from radians to degrees is generally inexact; users should
     * <i>not</i> expect <code>cos(toRadians(90.0))</code> to exactly equal <code>0.0</code>.
     *
     * @param angrad an angle, in radians
     *
     * @return the measurement of the angle <code>angrad</code> in degrees.
     *
     * @since 1.2
     */
    public static double toDegrees(double angrad) {
        return Math.toDegrees(angrad);
    }

    /**
     * Returns Euler's number <i>e</i> raised to the power of a <code>double</code> value.  Special
     * cases:
     * 
     * <ul>
     * <li>
     * If the argument is NaN, the result is NaN.
     * </li>
     * <li>
     * If the argument is positive infinity, then the result is  positive infinity.
     * </li>
     * <li>
     * If the argument is negative infinity, then the result is  positive zero.
     * </li>
     * </ul>
     * 
     * <p>
     * A result must be within 1 ulp of the correctly rounded result.  Results must be
     * semi-monotonic.
     * </p>
     *
     * @param a the exponent to raise <i>e</i> to.
     *
     * @return the value <i>e</i><sup><code>a</code></sup>,  where <i>e</i> is the base of the
     *         natural logarithms.
     */
    public static double exp(double a) {
        return Math.exp(a);
    }

    /**
     * Returns the natural logarithm (base <i>e</i>) of a <code>double</code> value.  Special
     * cases:
     * 
     * <ul>
     * <li>
     * If the argument is NaN or less than zero, then the result  is NaN.
     * </li>
     * <li>
     * If the argument is positive infinity, then the result is  positive infinity.
     * </li>
     * <li>
     * If the argument is positive zero or negative zero, then the  result is negative infinity.
     * </li>
     * </ul>
     * 
     * <p>
     * A result must be within 1 ulp of the correctly rounded result.  Results must be
     * semi-monotonic.
     * </p>
     *
     * @param a a number greater than <code>0.0</code>.
     *
     * @return the value ln&nbsp;<code>a</code>, the natural logarithm of <code>a</code>.
     */
    public static double log(double a) {
        return Math.log(a);
    }

    /**
     * Returns the correctly rounded positive square root of a  <code>double</code> value. Special
     * cases:
     * 
     * <ul>
     * <li>
     * If the argument is NaN or less than zero, then the result  is NaN.
     * </li>
     * <li>
     * If the argument is positive infinity, then the result is positive  infinity.
     * </li>
     * <li>
     * If the argument is positive zero or negative zero, then the  result is the same as the
     * argument.
     * </li>
     * </ul>
     * 
     * Otherwise, the result is the <code>double</code> value closest to  the true mathematical
     * square root of the argument value.
     *
     * @param a a value. <!--@return  the value of &radic;&nbsp;<code>a</code>.-->
     *
     * @return the positive square root of <code>a</code>. If the argument is NaN or less than
     *         zero, the result is NaN.
     */
    public static double sqrt(double a) {
        return Math.sqrt(a);
    }

    /**
     * Computes the remainder operation on two arguments as prescribed  by the IEEE 754 standard.
     * The remainder value is mathematically equal to
     * <code>f1&nbsp;-&nbsp;f2</code>&nbsp;&times;&nbsp;<i>n</i>, where <i>n</i> is the
     * mathematical integer closest to the exact  mathematical value of the quotient
     * <code>f1/f2</code>, and if two  mathematical integers are equally close to
     * <code>f1/f2</code>,  then <i>n</i> is the integer that is even. If the remainder is  zero,
     * its sign is the same as the sign of the first argument.  Special cases:
     * 
     * <ul>
     * <li>
     * If either argument is NaN, or the first argument is infinite,  or the second argument is
     * positive zero or negative zero, then the  result is NaN.
     * </li>
     * <li>
     * If the first argument is finite and the second argument is  infinite, then the result is the
     * same as the first argument.
     * </li>
     * </ul>
     * 
     *
     * @param f1 the dividend.
     * @param f2 the divisor.
     *
     * @return the remainder when <code>f1</code> is divided by <code>f2</code>.
     */
    public static double IEEEremainder(double f1, double f2) {
        return Math.IEEEremainder(f1, f2);
    }

    /**
     * Returns the smallest (closest to negative infinity)  <code>double</code> value that is not
     * less than the argument and is  equal to a mathematical integer. Special cases:
     * 
     * <ul>
     * <li>
     * If the argument value is already equal to a mathematical  integer, then the result is the
     * same as the argument.
     * </li>
     * <li>
     * If the argument is NaN or an infinity or positive zero or negative  zero, then the result is
     * the same as the argument.
     * </li>
     * <li>
     * If the argument value is less than zero but greater than -1.0,  then the result is negative
     * zero.
     * </li>
     * </ul>
     * 
     * Note that the value of <code>Math.ceil(x)</code> is exactly the  value of
     * <code>-Math.floor(-x)</code>.
     *
     * @param a a value. <!--@return  the value &lceil;&nbsp;<code>a</code>&nbsp;&rceil;.-->
     *
     * @return the smallest (closest to negative infinity)  floating-point value that is not less
     *         than the argument and is equal to a mathematical integer.
     */
    public static double ceil(double a) {
        return Math.ceil(a);
    }

    /**
     * Returns the largest (closest to positive infinity)  <code>double</code> value that is not
     * greater than the argument and  is equal to a mathematical integer. Special cases:
     * 
     * <ul>
     * <li>
     * If the argument value is already equal to a mathematical  integer, then the result is the
     * same as the argument.
     * </li>
     * <li>
     * If the argument is NaN or an infinity or positive zero or  negative zero, then the result is
     * the same as the argument.
     * </li>
     * </ul>
     * 
     *
     * @param a a value. <!--@return  the value &lfloor;&nbsp;<code>a</code>&nbsp;&rfloor;.-->
     *
     * @return the largest (closest to positive infinity)  floating-point value that is not greater
     *         than the argument and is equal to a mathematical integer.
     */
    public static double floor(double a) {
        return Math.floor(a);
    }

    /**
     * Returns the <code>double</code> value that is closest in value to the argument and is equal
     * to a mathematical integer. If two <code>double</code> values that are mathematical integers
     * are equally close, the result is the integer value that is even. Special cases:
     * 
     * <ul>
     * <li>
     * If the argument value is already equal to a mathematical  integer, then the result is the
     * same as the argument.
     * </li>
     * <li>
     * If the argument is NaN or an infinity or positive zero or negative  zero, then the result is
     * the same as the argument.
     * </li>
     * </ul>
     * 
     *
     * @param a a <code>double</code> value.
     *
     * @return the closest floating-point value to <code>a</code> that is equal to a mathematical
     *         integer.
     */
    public static double rint(double a) {
        return Math.rint(a);
    }

    /**
     * Converts rectangular coordinates (<code>x</code>,&nbsp;<code>y</code>) to polar
     * (r,&nbsp;<i>theta</i>). This method computes the phase <i>theta</i> by computing an arc
     * tangent of <code>y/x</code> in the range of -<i>pi</i> to <i>pi</i>. Special  cases:
     * 
     * <ul>
     * <li>
     * If either argument is NaN, then the result is NaN.
     * </li>
     * <li>
     * If the first argument is positive zero and the second argument  is positive, or the first
     * argument is positive and finite and the  second argument is positive infinity, then the
     * result is positive  zero.
     * </li>
     * <li>
     * If the first argument is negative zero and the second argument  is positive, or the first
     * argument is negative and finite and the  second argument is positive infinity, then the
     * result is negative zero.
     * </li>
     * <li>
     * If the first argument is positive zero and the second argument  is negative, or the first
     * argument is positive and finite and the  second argument is negative infinity, then the
     * result is the  <code>double</code> value closest to <i>pi</i>.
     * </li>
     * <li>
     * If the first argument is negative zero and the second argument  is negative, or the first
     * argument is negative and finite and the  second argument is negative infinity, then the
     * result is the  <code>double</code> value closest to -<i>pi</i>.
     * </li>
     * <li>
     * If the first argument is positive and the second argument is  positive zero or negative
     * zero, or the first argument is positive  infinity and the second argument is finite, then
     * the result is the  <code>double</code> value closest to <i>pi</i>/2.
     * </li>
     * <li>
     * If the first argument is negative and the second argument is  positive zero or negative
     * zero, or the first argument is negative  infinity and the second argument is finite, then
     * the result is the  <code>double</code> value closest to -<i>pi</i>/2.
     * </li>
     * <li>
     * If both arguments are positive infinity, then the result is the  <code>double</code> value
     * closest to <i>pi</i>/4.
     * </li>
     * <li>
     * If the first argument is positive infinity and the second argument  is negative infinity,
     * then the result is the <code>double</code>  value closest to 3<i>pi</i>/4.
     * </li>
     * <li>
     * If the first argument is negative infinity and the second argument  is positive infinity,
     * then the result is the <code>double</code> value  closest to -<i>pi</i>/4.
     * </li>
     * <li>
     * If both arguments are negative infinity, then the result is the  <code>double</code> value
     * closest to -3<i>pi</i>/4.
     * </li>
     * </ul>
     * 
     * <p>
     * A result must be within 2 ulps of the correctly rounded result.  Results must be
     * semi-monotonic.
     * </p>
     *
     * @param y the ordinate coordinate
     * @param x the abscissa coordinate
     *
     * @return the <i>theta</i> component of the point (<i>r</i>,&nbsp;<i>theta</i>) in polar
     *         coordinates that corresponds to the point (<i>x</i>,&nbsp;<i>y</i>) in Cartesian
     *         coordinates.
     */
    public static double atan2(double y, double x) {
        return Math.atan2(y, x);
    }

    /**
     * Returns the value of the first argument raised to the power of the second argument. Special
     * cases:
     * 
     * <ul>
     * <li>
     * If the second argument is positive or negative zero, then the  result is 1.0.
     * </li>
     * <li>
     * If the second argument is 1.0, then the result is the same as the  first argument.
     * </li>
     * <li>
     * If the second argument is NaN, then the result is NaN.
     * </li>
     * <li>
     * If the first argument is NaN and the second argument is nonzero,  then the result is NaN.
     * </li>
     * <li>
     * If
     * 
     * <ul>
     * <li>
     * the absolute value of the first argument is greater than 1 and the second argument is
     * positive infinity, or
     * </li>
     * <li>
     * the absolute value of the first argument is less than 1 and the second argument is negative
     * infinity,
     * </li>
     * </ul>
     * 
     * then the result is positive infinity.
     * </li>
     * <li>
     * If
     * 
     * <ul>
     * <li>
     * the absolute value of the first argument is greater than 1 and  the second argument is
     * negative infinity, or
     * </li>
     * <li>
     * the absolute value of the  first argument is less than 1 and the second argument is positive
     * infinity,
     * </li>
     * </ul>
     * 
     * then the result is positive zero.
     * </li>
     * <li>
     * If the absolute value of the first argument equals 1 and the  second argument is infinite,
     * then the result is NaN.
     * </li>
     * <li>
     * If
     * 
     * <ul>
     * <li>
     * the first argument is positive zero and the second argument is greater than zero, or
     * </li>
     * <li>
     * the first argument is positive infinity and the second argument is less than zero,
     * </li>
     * </ul>
     * 
     * then the result is positive zero.
     * </li>
     * <li>
     * If
     * 
     * <ul>
     * <li>
     * the first argument is positive zero and the second argument is less than zero, or
     * </li>
     * <li>
     * the first argument is positive infinity and the second argument is greater than zero,
     * </li>
     * </ul>
     * 
     * then the result is positive infinity.
     * </li>
     * <li>
     * If
     * 
     * <ul>
     * <li>
     * the first argument is negative zero and the second argument is greater than zero but not a
     * finite odd integer, or
     * </li>
     * <li>
     * the first argument is negative infinity and the second argument is less than zero but not a
     * finite odd integer,
     * </li>
     * </ul>
     * 
     * then the result is positive zero.
     * </li>
     * <li>
     * If
     * 
     * <ul>
     * <li>
     * the first argument is negative zero and the second argument is a positive finite odd
     * integer, or
     * </li>
     * <li>
     * the first argument is negative infinity and the second argument is a negative finite odd
     * integer,
     * </li>
     * </ul>
     * 
     * then the result is negative zero.
     * </li>
     * <li>
     * If
     * 
     * <ul>
     * <li>
     * the first argument is negative zero and the second argument is less than zero but not a
     * finite odd integer, or
     * </li>
     * <li>
     * the first argument is negative infinity and the second argument is greater than zero but not
     * a finite odd integer,
     * </li>
     * </ul>
     * 
     * then the result is positive infinity.
     * </li>
     * <li>
     * If
     * 
     * <ul>
     * <li>
     * the first argument is negative zero and the second argument is a negative finite odd
     * integer, or
     * </li>
     * <li>
     * the first argument is negative infinity and the second argument is a positive finite odd
     * integer,
     * </li>
     * </ul>
     * 
     * then the result is negative infinity.
     * </li>
     * <li>
     * If the first argument is finite and less than zero
     * 
     * <ul>
     * <li>
     * if the second argument is a finite even integer, the result is equal to the result of
     * raising the absolute value of the first argument to the power of the second argument
     * </li>
     * <li>
     * if the second argument is a finite odd integer, the result is equal to the negative of the
     * result of raising the absolute value of the first argument to the power of the second
     * argument
     * </li>
     * <li>
     * if the second argument is finite and not an integer, then the result is NaN.
     * </li>
     * </ul>
     * 
     * </li>
     * <li>
     * If both arguments are integers, then the result is exactly equal  to the mathematical result
     * of raising the first argument to the power  of the second argument if that result can in
     * fact be represented  exactly as a <code>double</code> value.
     * </li>
     * </ul>
     * 
     * <p>
     * (In the foregoing descriptions, a floating-point value is considered to be an integer if and
     * only if it is finite and a fixed point of the method {@link #ceil <tt>ceil</tt>} or,
     * equivalently, a fixed point of the method {@link #floor <tt>floor</tt>}. A value is a fixed
     * point of a one-argument method if and only if the result of applying the method to the
     * value is equal to the value.)
     * </p>
     * 
     * <p>
     * A result must be within 1 ulp of the correctly rounded result.  Results must be
     * semi-monotonic.
     * </p>
     *
     * @param a the base.
     * @param b the exponent.
     *
     * @return the value <code>a<sup>b</sup></code>.
     */
    public static double pow(double a, double b) {
        return Math.pow(a, b);
    }

    /**
     * Returns the closest <code>int</code> to the argument. The  result is rounded to an integer
     * by adding 1/2, taking the  floor of the result, and casting the result to type
     * <code>int</code>.  In other words, the result is equal to the value of the expression:
     * 
     * <p>
     * <pre>(int)Math.floor(a + 0.5f)</pre>
     * </p>
     * 
     * <p>
     * Special cases:
     * 
     * <ul>
     * <li>
     * If the argument is NaN, the result is 0.
     * </li>
     * <li>
     * If the argument is negative infinity or any value less than or  equal to the value of
     * <code>Integer.MIN_VALUE</code>, the result is  equal to the value of
     * <code>Integer.MIN_VALUE</code>.
     * </li>
     * <li>
     * If the argument is positive infinity or any value greater than or  equal to the value of
     * <code>Integer.MAX_VALUE</code>, the result is  equal to the value of
     * <code>Integer.MAX_VALUE</code>.
     * </li>
     * </ul>
     * </p>
     *
     * @param a a floating-point value to be rounded to an integer.
     *
     * @return the value of the argument rounded to the nearest <code>int</code> value.
     *
     * @see java.lang.Integer#MAX_VALUE
     * @see java.lang.Integer#MIN_VALUE
     */
    public static int round(float a) {
        return Math.round(a);
    }

    /**
     * Returns the closest <code>long</code> to the argument. The result  is rounded to an integer
     * by adding 1/2, taking the floor of the  result, and casting the result to type
     * <code>long</code>. In other  words, the result is equal to the value of the expression:
     * 
     * <p>
     * <pre>(long)Math.floor(a + 0.5d)</pre>
     * </p>
     * 
     * <p>
     * Special cases:
     * 
     * <ul>
     * <li>
     * If the argument is NaN, the result is 0.
     * </li>
     * <li>
     * If the argument is negative infinity or any value less than or  equal to the value of
     * <code>Long.MIN_VALUE</code>, the result is  equal to the value of
     * <code>Long.MIN_VALUE</code>.
     * </li>
     * <li>
     * If the argument is positive infinity or any value greater than or  equal to the value of
     * <code>Long.MAX_VALUE</code>, the result is  equal to the value of
     * <code>Long.MAX_VALUE</code>.
     * </li>
     * </ul>
     * </p>
     *
     * @param a a floating-point value to be rounded to a  <code>long</code>.
     *
     * @return the value of the argument rounded to the nearest <code>long</code> value.
     *
     * @see java.lang.Long#MAX_VALUE
     * @see java.lang.Long#MIN_VALUE
     */
    public static long round(double a) {
        return Math.round(a);
    }

    /**
     * Returns a <code>double</code> value with a positive sign, greater  than or equal to
     * <code>0.0</code> and less than <code>1.0</code>.  Returned values are chosen pseudorandomly
     * with (approximately)  uniform distribution from that range.
     * 
     * <p>
     * When this method is first called, it creates a single new  pseudorandom-number generator,
     * exactly as if by the expression
     * <blockquote>
     * <pre>new java.util.Random</pre>
     * </blockquote>
     * This new pseudorandom-number generator is used thereafter for all  calls to this method and
     * is used nowhere else.
     * </p>
     * 
     * <p>
     * This method is properly synchronized to allow correct use by more  than one thread. However,
     * if many threads need to generate  pseudorandom numbers at a great rate, it may reduce
     * contention for  each thread to have its own pseudorandom-number generator.
     * </p>
     *
     * @return a pseudorandom <code>double</code> greater than or equal  to <code>0.0</code> and
     *         less than <code>1.0</code>.
     *
     * @see java.util.Random#nextDouble()
     */
    public static double random() {
        return Math.random();
    }

    /**
     * Returns the absolute value of an <code>int</code> value. If the argument is not negative,
     * the argument is returned. If the argument is negative, the negation of the argument is
     * returned.
     * 
     * <p>
     * Note that if the argument is equal to the value of  <code>Integer.MIN_VALUE</code>, the most
     * negative representable  <code>int</code> value, the result is that same value, which is
     * negative.
     * </p>
     *
     * @param a the argument whose absolute value is to be determined
     *
     * @return the absolute value of the argument.
     *
     * @see java.lang.Integer#MIN_VALUE
     */
    public static int abs(int a) {
        return Math.abs(a);
    }

    /**
     * Returns the absolute value of a <code>long</code> value. If the argument is not negative,
     * the argument is returned. If the argument is negative, the negation of the argument is
     * returned.
     * 
     * <p>
     * Note that if the argument is equal to the value of  <code>Long.MIN_VALUE</code>, the most
     * negative representable  <code>long</code> value, the result is that same value, which is
     * negative.
     * </p>
     *
     * @param a the argument whose absolute value is to be determined
     *
     * @return the absolute value of the argument.
     *
     * @see java.lang.Long#MIN_VALUE
     */
    public static long abs(long a) {
        return Math.abs(a);
    }

    /**
     * Returns the absolute value of a <code>float</code> value. If the argument is not negative,
     * the argument is returned. If the argument is negative, the negation of the argument is
     * returned. Special cases:
     * 
     * <ul>
     * <li>
     * If the argument is positive zero or negative zero, the  result is positive zero.
     * </li>
     * <li>
     * If the argument is infinite, the result is positive infinity.
     * </li>
     * <li>
     * If the argument is NaN, the result is NaN.
     * </li>
     * </ul>
     * 
     * In other words, the result is the same as the value of the expression:
     * 
     * <p>
     * <pre>Float.intBitsToFloat(0x7fffffff & Float.floatToIntBits(a))</pre>
     * </p>
     *
     * @param a the argument whose absolute value is to be determined
     *
     * @return the absolute value of the argument.
     */
    public static float abs(float a) {
        return Math.abs(a);
    }

    /**
     * Returns the absolute value of a <code>double</code> value. If the argument is not negative,
     * the argument is returned. If the argument is negative, the negation of the argument is
     * returned. Special cases:
     * 
     * <ul>
     * <li>
     * If the argument is positive zero or negative zero, the result  is positive zero.
     * </li>
     * <li>
     * If the argument is infinite, the result is positive infinity.
     * </li>
     * <li>
     * If the argument is NaN, the result is NaN.
     * </li>
     * </ul>
     * 
     * In other words, the result is the same as the value of the expression:
     * 
     * <p>
     * <code>Double.longBitsToDouble((Double.doubleToLongBits(a)&lt;&lt;1)&gt;&gt;&gt;1)</code>
     * </p>
     *
     * @param a the argument whose absolute value is to be determined
     *
     * @return the absolute value of the argument.
     */
    public static double abs(double a) {
        return Math.abs(a);
    }

    /**
     * Returns the greater of two <code>int</code> values. That is, the  result is the argument
     * closer to the value of  <code>Integer.MAX_VALUE</code>. If the arguments have the same
     * value,  the result is that same value.
     *
     * @param a an argument.
     * @param b another argument.
     *
     * @return the larger of <code>a</code> and <code>b</code>.
     *
     * @see java.lang.Long#MAX_VALUE
     */
    public static int max(int a, int b) {
        return Math.max(a, b);
    }

    /**
     * Returns the greater of two <code>long</code> values. That is, the  result is the argument
     * closer to the value of  <code>Long.MAX_VALUE</code>. If the arguments have the same value,
     * the result is that same value.
     *
     * @param a an argument.
     * @param b another argument.
     *
     * @return the larger of <code>a</code> and <code>b</code>.
     *
     * @see java.lang.Long#MAX_VALUE
     */
    public static long max(long a, long b) {
        return Math.max(a, b);
    }

    /**
     * Returns the greater of two <code>float</code> values.  That is, the result is the argument
     * closer to positive infinity. If the arguments have the same value, the result is that same
     * value. If either value is NaN, then the result is NaN.  Unlike the the numerical comparison
     * operators, this method considers negative zero to be strictly smaller than positive zero.
     * If one argument is positive zero and the other negative zero, the result is positive zero.
     *
     * @param a an argument.
     * @param b another argument.
     *
     * @return the larger of <code>a</code> and <code>b</code>.
     */
    public static float max(float a, float b) {
        return Math.max(a, b);
    }

    /**
     * Returns the greater of two <code>double</code> values.  That is, the result is the argument
     * closer to positive infinity. If the arguments have the same value, the result is that same
     * value. If either value is NaN, then the result is NaN.  Unlike the the numerical comparison
     * operators, this method considers negative zero to be strictly smaller than positive zero.
     * If one argument is positive zero and the other negative zero, the result is positive zero.
     *
     * @param a an argument.
     * @param b another argument.
     *
     * @return the larger of <code>a</code> and <code>b</code>.
     */
    public static double max(double a, double b) {
        return Math.max(a, b);
    }

    /**
     * Returns the smaller of two <code>int</code> values. That is, the result the argument closer
     * to the value of <code>Integer.MIN_VALUE</code>.  If the arguments have the same value, the
     * result is that same value.
     *
     * @param a an argument.
     * @param b another argument.
     *
     * @return the smaller of <code>a</code> and <code>b</code>.
     *
     * @see java.lang.Long#MIN_VALUE
     */
    public static int min(int a, int b) {
        return Math.min(a, b);
    }

    /**
     * Returns the smaller of two <code>long</code> values. That is, the result is the argument
     * closer to the value of <code>Long.MIN_VALUE</code>. If the arguments have the same value,
     * the result is that same value.
     *
     * @param a an argument.
     * @param b another argument.
     *
     * @return the smaller of <code>a</code> and <code>b</code>.
     *
     * @see java.lang.Long#MIN_VALUE
     */
    public static long min(long a, long b) {
        return Math.min(a, b);
    }

    /**
     * Returns the smaller of two <code>float</code> values.  That is, the result is the value
     * closer to negative infinity. If the arguments have the same value, the result is that same
     * value. If either value is NaN, then the result is NaN.  Unlike the the numerical comparison
     * operators, this method considers negative zero to be strictly smaller than positive zero.
     * If one argument is positive zero and the other is negative zero, the result is negative
     * zero.
     *
     * @param a an argument.
     * @param b another argument.
     *
     * @return the smaller of <code>a</code> and <code>b.</code>
     */
    public static float min(float a, float b) {
        return Math.min(a, b);
    }

    /**
     * Returns the smaller of two <code>double</code> values.  That is, the result is the value
     * closer to negative infinity. If the arguments have the same value, the result is that same
     * value. If either value is NaN, then the result is NaN.  Unlike the the numerical comparison
     * operators, this method considers negative zero to be strictly smaller than positive zero.
     * If one argument is positive zero and the other is negative zero, the result is negative
     * zero.
     *
     * @param a an argument.
     * @param b another argument.
     *
     * @return the smaller of <code>a</code> and <code>b</code>.
     */
    public static double min(double a, double b) {
        return Math.min(a, b);
    }

    public static boolean isPowerOfTwo(int intValue) {
        if (intValue == 0) {
            return false;
        }

        while ((intValue & 1) == 0) {
            intValue = intValue >>> 1;
        }

        return intValue == 1;
    }

    public static boolean isPowerOfTwo(long longValue) {
        if (longValue == 0L) {
            return false;
        }

        while ((longValue & 1L) == 0L) {
            longValue = longValue >>> 1;
        }

        return longValue == 1L;
    }

    public static boolean isPowerOfTwo(BigInteger bintValue) {
        int bitIndex = bintValue.getLowestSetBit();

        if (bitIndex < 0) {
            return false;
        }

        return bintValue.clearBit(bitIndex).equals(BigInteger.ZERO);
    }
}
