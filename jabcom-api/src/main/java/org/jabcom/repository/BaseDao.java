package org.jabcom.repository;

import java.io.Serializable;

public interface BaseDao<K extends Serializable, T extends Serializable> {

    T fetchByKey(final K key);

    T fetchByKey(final String stringKey);

    void delete(final K key);

    void delete(final String key);

    void save(final T e);

}
