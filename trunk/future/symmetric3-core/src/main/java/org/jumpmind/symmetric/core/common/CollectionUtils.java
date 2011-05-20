package org.jumpmind.symmetric.core.common;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class CollectionUtils {

    /**
     * Adds all elements in the array to the given collection.
     * 
     * @param collection
     *            the collection to add to, may not be null
     * @param elements
     *            the array of elements to add, may not be null
     * @throws NullPointerException
     *             if the collection or array is null
     */
    public static <T> void addAll(Collection<T> collection, T[] elements) {
        for (int i = 0, size = elements.length; i < size; i++) {
            collection.add(elements[i]);
        }
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
     * ArrayUtils.addAll(array1, null)   = cloned copy of array1
     * ArrayUtils.addAll(null, array2)   = cloned copy of array2
     * ArrayUtils.addAll([], [])         = []
     * </pre>
     * 
     * @param array1
     *            the first array whose elements are added to the new array.
     * @param array2
     *            the second array whose elements are added to the new array.
     * @return The new int[] array.
     * @since 2.1
     */
    public static int[] addAll(int[] array1, int[] array2) {
        if (array1 == null) {
            return clone(array2);
        } else if (array2 == null) {
            return clone(array1);
        }
        int[] joinedArray = new int[array1.length + array2.length];
        System.arraycopy(array1, 0, joinedArray, 0, array1.length);
        System.arraycopy(array2, 0, joinedArray, array1.length, array2.length);
        return joinedArray;
    }

    /**
     * <p>
     * Clones an array returning a typecast result and handling
     * <code>null</code>.
     * </p>
     * 
     * <p>
     * This method returns <code>null</code> for a <code>null</code> input
     * array.
     * </p>
     * 
     * @param array
     *            the array to clone, may be <code>null</code>
     * @return the cloned array, <code>null</code> if <code>null</code> input
     */
    public static int[] clone(int[] array) {
        if (array == null) {
            return null;
        }
        return (int[]) array.clone();
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
    public static <T> T[] addAll(T[] array1, T[] array2) {
        if (array1 == null) {
            return clone(array2);
        } else if (array2 == null) {
            return clone(array1);
        }
        @SuppressWarnings("unchecked")
        T[] joinedArray = (T[]) Array.newInstance(array1.getClass().getComponentType(),
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
    public static <T> T[] clone(T[] array) {
        if (array == null) {
            return null;
        }
        return (T[]) array.clone();
    }

    public static <T> List<T> selectList(Class<T> collectionType, Class<?>[] candidateTypes,
            Collection<?> collection) {
        List<T> list = new ArrayList<T>();
        for (Object object : collection) {
            for (Class<?> type : candidateTypes) {
                if (type.isAssignableFrom(object.getClass())) {
                    list.add((T) object);
                }
            }
        }
        return list;
    }

}
