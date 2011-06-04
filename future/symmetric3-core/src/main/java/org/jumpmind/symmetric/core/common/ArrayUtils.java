package org.jumpmind.symmetric.core.common;

import java.lang.reflect.Array;

public abstract class ArrayUtils {

    /**
     * <p>
     * Produces a new array containing the elements between the start and end
     * indices.
     * </p>
     * 
     * <p>
     * The start index is inclusive, the end index exclusive. Null array input
     * produces null output.
     * </p>
     * 
     * <p>
     * The component type of the subarray is always the same as that of the
     * input array. Thus, if the input is an array of type <code>Date</code>,
     * the following usage is envisaged:
     * </p>
     * 
     * <pre>
     * Date[] someDates = (Date[]) ArrayUtils.subarray(allDates, 2, 5);
     * </pre>
     * 
     * @param array
     *            the array
     * @param startIndexInclusive
     *            the starting index. Undervalue (&lt;0) is promoted to 0,
     *            overvalue (&gt;array.length) results in an empty array.
     * @param endIndexExclusive
     *            elements up to endIndex-1 are present in the returned
     *            subarray. Undervalue (&lt; startIndex) produces empty array,
     *            overvalue (&gt;array.length) is demoted to array length.
     * @return a new array containing the elements between the start and end
     *         indices.
     * @since 2.1
     */
    public static String[] subarray(String[] array, int startIndexInclusive, int endIndexExclusive) {
        if (array == null) {
            return null;
        }
        if (startIndexInclusive < 0) {
            startIndexInclusive = 0;
        }
        if (endIndexExclusive > array.length) {
            endIndexExclusive = array.length;
        }
        int newSize = endIndexExclusive - startIndexInclusive;
        String[] subarray = new String[newSize];
        System.arraycopy(array, startIndexInclusive, subarray, 0, newSize);
        return subarray;
    }

    /**
     * <p>
     * Adds all the elements of the given arrays into a new array.
     * </p>
     * <p>
     * The new array contains all of the element of <code>array1</code> followed
     * by all of the elements <code>array2</code>. When an array is returned, it
     * is always a new array.
     * </p>
     * 
     * <pre>
     * ArrayUtils.addAll(null, null)     = null
     * ArrayUtils.addAll(array1, null)   = cloned copy of array1
     * ArrayUtils.addAll(null, array2)   = cloned copy of array2
     * ArrayUtils.addAll([], [])         = []
     * ArrayUtils.addAll([null], [null]) = [null, null]
     * ArrayUtils.addAll(["a", "b", "c"], ["1", "2", "3"]) = ["a", "b", "c", "1", "2", "3"]
     * </pre>
     * 
     * @param array1
     *            the first array whose elements are added to the new array, may
     *            be <code>null</code>
     * @param array2
     *            the second array whose elements are added to the new array,
     *            may be <code>null</code>
     * @return The new array, <code>null</code> if <code>null</code> array
     *         inputs. The type of the new array is the type of the first array.
     * @since 2.1
     */
    public static Object[] addAll(Object[] array1, Object[] array2) {
        if (array1 == null) {
            return clone(array2);
        } else if (array2 == null) {
            return clone(array1);
        }
        Object[] joinedArray = (Object[]) Array.newInstance(array1.getClass().getComponentType(),
                array1.length + array2.length);
        System.arraycopy(array1, 0, joinedArray, 0, array1.length);
        System.arraycopy(array2, 0, joinedArray, array1.length, array2.length);
        return joinedArray;
    }

    /**
     * <p>
     * Shallow clones an array returning a typecast result and handling
     * <code>null</code>.
     * </p>
     * 
     * <p>
     * The objects in the array are not cloned, thus there is no special
     * handling for multi-dimensional arrays.
     * </p>
     * 
     * <p>
     * This method returns <code>null</code> for a <code>null</code> input
     * array.
     * </p>
     * 
     * @param array
     *            the array to shallow clone, may be <code>null</code>
     * @return the cloned array, <code>null</code> if <code>null</code> input
     */
    public static Object[] clone(Object[] array) {
        if (array == null) {
            return null;
        }
        return (Object[]) array.clone();
    }

    /**
     * Translate an array of {@link Object} to an array of {@link String} by
     * creating a new array of {@link String} and putting each of the objects
     * into the array by calling {@link Object#toString()}
     * 
     * @param orig
     *            the original array
     * @return a newly constructed string array
     */
    public static String[] toStringArray(Object[] orig) {
        String[] array = null;
        if (orig != null) {
            array = new String[orig.length];
            for (int i = 0; i < orig.length; i++) {
                if (orig[i] != null) {
                    array[i] = orig[i].toString();
                }
            }
        }
        return array;
    }
}
