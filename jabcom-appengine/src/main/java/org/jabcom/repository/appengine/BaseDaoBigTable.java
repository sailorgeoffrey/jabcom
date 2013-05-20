package org.jabcom.repository.appengine;

import com.google.appengine.api.datastore.Key;
import org.jabcom.repository.BaseDao;

import java.io.Serializable;

public interface BaseDaoBigTable<T extends Serializable> extends BaseDao<Key, T> {
}
