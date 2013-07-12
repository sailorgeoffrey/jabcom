package org.jabcom.repository.appengine;

import com.google.appengine.api.datastore.*;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import org.jabcom.annotation.Id;
import org.jabcom.annotation.Parent;
import org.jabcom.repository.BaseDao;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Test for BigTable operations.
 */
public class BigTableDaoTest {

    private final LocalServiceTestHelper helper;
    private final DatastoreService datastore;

    {
        helper = new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());
        datastore = DatastoreServiceFactory.getDatastoreService();
    }

    @Before
    public void setUp() {
        helper.setUp();
    }

    @After
    public void tearDown() {
        helper.tearDown();
    }

    public static final class TestParentObject implements Serializable {

        @Id
        private Key key;

        private String name;

        private TestChildObject childObject;

        public Key getKey() {
            return key;
        }

        public void setKey(Key key) {
            this.key = key;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static final class TestChildObject implements Serializable {

        @Id
        private Key key;

        @Parent
        private Key parentKey;

        private String name;

        public Key getKey() {
            return key;
        }

        public void setKey(Key key) {
            this.key = key;
        }

        public void setParentKey(Key parentKey) {
            this.parentKey = parentKey;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }


    @Test
    public void testFetchLowLevelAPI() throws Exception {

        Entity domainObject = new Entity("TestParentObject", "xyz");
        domainObject.setProperty("propertyOne", 999);
        domainObject.setProperty("propertyTwo", "foo");
        domainObject.setProperty("propertyThree", new Date());
        datastore.put(domainObject);
        Key domainObjectKey = domainObject.getKey();
        Assert.assertNotNull(domainObjectKey);

        Entity fetched = datastore.get(domainObjectKey);
        Assert.assertNotNull(fetched);

    }

    @Test
    public void testParentFetchLowLevelAPI() throws Exception {

        Query q;
        PreparedQuery pq;

        Entity domainObject = new Entity("TestParentObject", "xyz");
        domainObject.setProperty("propertyOne", 999);
        domainObject.setProperty("propertyTwo", "foo");
        domainObject.setProperty("propertyThree", new Date());
        datastore.put(domainObject);
        Key domainObjectKey = domainObject.getKey();
        Assert.assertNotNull(domainObjectKey);

        Entity custody1 = new Entity("Custody", domainObjectKey);
        custody1.setProperty("custodian", "Geoff");
        datastore.put(custody1);
        Key custodyKey = custody1.getKey();
        Assert.assertNotNull(custodyKey);

        q = new Query("Custody");
        q.setAncestor(domainObjectKey);
        pq = datastore.prepare(q);
        Assert.assertNotNull(pq.asSingleEntity());

        Entity custody2 = new Entity("Custody", domainObjectKey);
        custody2.setProperty("custodian", "Alex");
        datastore.put(custody2);

        q = new Query("Custody");
        q.setAncestor(domainObjectKey);
        pq = datastore.prepare(q);

        List<Entity> results = new ArrayList<Entity>();
        for (Entity e : pq.asIterable()) {
            results.add(e);
        }
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(custody1, results.get(0));

    }

    @Test
    public void testGenericDaoOperations() throws Exception {

        BaseDao<Key, TestParentObject> parentDao =
                new BaseDaoBigTable<TestParentObject>(datastore, TestParentObject.class);
        BaseDao<Key, TestChildObject> childDao =
                new BaseDaoBigTable<TestChildObject>(datastore, TestChildObject.class);

        TestParentObject parentObject = new TestParentObject();
        Key parentKey = KeyFactory.createKey("parent", 1L);
        parentObject.setKey(parentKey);
        parentObject.setName("first");

        parentDao.save(parentObject);
        Assert.assertNotNull(parentObject.getName());

        TestChildObject childObject = new TestChildObject();
        childObject.setParentKey(parentKey);
        childObject.setName("child of first");

        childDao.save(childObject);
        Assert.assertNotNull(childObject.getKey());

        TestParentObject fetched = parentDao.fetchByKey(parentKey);
        Assert.assertNotNull(fetched);
        Assert.assertEquals("first", fetched.getName());

        fetched.setName("new name");
        parentDao.save(fetched);

        TestParentObject modified = parentDao.fetchByKey(KeyFactory.keyToString(parentKey));
        Assert.assertNotNull(modified);
        Assert.assertEquals("new name", fetched.getName());

        parentDao.delete(parentKey);
        TestParentObject deleted = parentDao.fetchByKey(parentKey);
        Assert.assertNull(deleted);

        TestParentObject another = new TestParentObject();
        another.setKey(KeyFactory.createKey("parent", 2L));
        another.setName("second");
        parentDao.save(another);

        TestParentObject anotherFetched = parentDao.fetchByKey(another.getKey());
        Assert.assertNotNull(anotherFetched);

        parentDao.delete(KeyFactory.keyToString(another.getKey()));

        TestParentObject anotherDeleted = parentDao.fetchByKey(another.getKey());
        Assert.assertNull(anotherDeleted);

    }

    private static final class BadClassOne implements Serializable {
        @Id
        private Key key;

        public Key getKey() {
            return key;
        }

        private BadClassOne() {
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testInaccessibleConstructorException() throws Exception {
        BaseDao<Key, BadClassOne> dao = new BaseDaoBigTable<BadClassOne>(datastore, BadClassOne.class);
        BadClassOne data = new BadClassOne();
        dao.save(data);
        Key key = data.getKey();
        Assert.assertNotNull(key);
        dao.fetchByKey(key);
    }

    private static final class BadClass implements Serializable {
        @Id
        private Key key;

        public Key getKey() {
            return key;
        }

        public BadClass(Key key) {
            this.key = key;
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testMissingConstructorException() throws Exception {
        BaseDao<Key, BadClass> dao = new BaseDaoBigTable<BadClass>(datastore, BadClass.class);
        BadClass data = new BadClass(KeyFactory.createKey("test", 1L));
        dao.save(data);
        Key key = data.getKey();
        Assert.assertNotNull(key);
        dao.fetchByKey(key);
    }

    @Test
    public void testEntitiesToObjects() throws Exception {
        new BaseDaoBigTable<Serializable>(datastore, TestParentObject.class) {
            {
                save(new TestParentObject());
                save(new TestParentObject());
                save(new TestParentObject());
                save(new TestParentObject());
                save(new TestParentObject());
                List<Entity> entities =
                        getDatastore().prepare(
                                new Query("TestParentObject")
                        ).asList(FetchOptions.Builder.withDefaults());
                Assert.assertEquals(5, entitiesToObjects(entities).size());
            }
        };
    }

    @Test
    public void testGetDataStore() throws Exception {
        new BaseDaoBigTable<Serializable>(datastore, Serializable.class) {
            {
                Assert.assertSame(datastore, getDatastore());
            }
        };
    }

}
