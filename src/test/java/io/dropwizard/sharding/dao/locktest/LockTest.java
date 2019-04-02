/*
 * Copyright 2016 Santanu Sinha <santanu.sinha@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.dropwizard.sharding.dao.locktest;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.dropwizard.sharding.dao.LookupDao;
import io.dropwizard.sharding.dao.RelationalDao;
import io.dropwizard.sharding.sharding.ShardManager;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.criterion.DetachedCriteria;
import org.hibernate.criterion.Restrictions;
import org.hibernate.exception.ConstraintViolationException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Test locking behavior
 */
public class LockTest {
    private List<SessionFactory> sessionFactories = Lists.newArrayList();

    private LookupDao<SomeLookupObject> lookupDao;
    private RelationalDao<SomeOtherObject> relationDao;

    private SessionFactory buildSessionFactory(String dbName) {
        Configuration configuration = new Configuration();
        configuration.setProperty("hibernate.dialect", "org.hibernate.dialect.H2Dialect");
        configuration.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
        configuration.setProperty("hibernate.connection.url", "jdbc:h2:mem:" + dbName);
        configuration.setProperty("hibernate.hbm2ddl.auto", "create");
        configuration.setProperty("hibernate.current_session_context_class", "managed");
        configuration.addAnnotatedClass(SomeLookupObject.class);
        configuration.addAnnotatedClass(SomeOtherObject.class);

        StandardServiceRegistry serviceRegistry
                = new StandardServiceRegistryBuilder().applySettings(
                configuration.getProperties()).build();
        return configuration.buildSessionFactory(serviceRegistry);
    }

    @Before
    public void before() {
        for (int i = 0; i < 2; i++) {
            SessionFactory sessionFactory = buildSessionFactory(String.format("db_%d", i));
            sessionFactories.add(sessionFactory);
        }
        final ShardManager shardManager = new ShardManager(sessionFactories.size());
        lookupDao = new LookupDao<>(sessionFactories, SomeLookupObject.class, shardManager, Integer::parseInt);
        relationDao = new RelationalDao<>(sessionFactories, SomeOtherObject.class, shardManager, Integer::parseInt);
    }

    @Test
    public void testLocking() throws Exception {
        SomeLookupObject p1 = SomeLookupObject.builder()
                                .myId("0")
                                .name("Parent 1")
                                .build();
        lookupDao.save(p1);
        System.out.println(lookupDao.get("0").get().getName());

        lookupDao.lockAndGetExecutor("0")
                .filter(parent -> !Strings.isNullOrEmpty(parent.getName()))
                .save(relationDao, parent -> SomeOtherObject.builder()
                        .my_id(parent.getMyId())
                        .value("Hello")
                        .build())
                .saveAll(relationDao,
                        parent -> IntStream.range(1,6)
                            .mapToObj(i -> SomeOtherObject.builder()
                                    .my_id(parent.getMyId())
                                    .value(String.format("Hello_%s", i))
                                    .build())
                        .collect(Collectors.toList())
                )
                .mutate(parent -> parent.setName("Changed"))
                .execute();

        Assert.assertEquals(p1.getMyId(), lookupDao.get("0").get().getMyId());
        Assert.assertEquals("Changed", lookupDao.get("0").get().getName());
        System.out.println(relationDao.get("0", 1L).get());
        Assert.assertEquals(6, relationDao.select("0", DetachedCriteria.forClass(SomeOtherObject.class), 0, 10).size());
        Assert.assertEquals("Hello",relationDao.get("0", 1L).get().getValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLockingFail() throws Exception {
        SomeLookupObject p1 = SomeLookupObject.builder()
                                .myId("0")
                                .build();
        lookupDao.save(p1);
        System.out.println(lookupDao.get("0").get().getName());

        lookupDao.lockAndGetExecutor("0")
                .filter(parent -> !Strings.isNullOrEmpty(parent.getName()))
                .save(relationDao, parent -> {
                    SomeOtherObject result = SomeOtherObject.builder()
                            .my_id(parent.getMyId())
                            .value("Hello")
                            .build();
                    parent.setName("Changed");
                    return result;
                })
                .mutate(parent -> parent.setName("Changed"))
                .execute();

    }



    @Test
    public void testPersist() throws Exception {
        SomeLookupObject p1 = SomeLookupObject.builder()
                .myId("0")
                .name("Parent 1")
                .build();

        lookupDao.saveAndGetExecutor(p1)
                .filter(parent -> !Strings.isNullOrEmpty(parent.getName()))
                .save(relationDao, parent -> SomeOtherObject.builder()
                        .my_id(parent.getMyId())
                        .value("Hello")
                        .build())
                .mutate(parent -> parent.setName("Changed"))
                .execute();

        Assert.assertEquals(p1.getMyId(), lookupDao.get("0").get().getMyId());
        Assert.assertEquals("Changed", lookupDao.get("0").get().getName());
    }

    @Test
    public void testUpdateById() throws Exception {
        SomeLookupObject p1 = SomeLookupObject.builder()
                .myId("0")
                .name("Parent 1")
                .build();

        SomeOtherObject c1 = relationDao.save(p1.getMyId(), SomeOtherObject.builder()
                .my_id(p1.getMyId())
                .value("Hello")
                .build()).get();


        lookupDao.saveAndGetExecutor(p1)
                .filter(parent -> !Strings.isNullOrEmpty(parent.getName()))
                .update(relationDao, c1.getId(), child -> {
                    child.setValue("Hello Changed");
                    return child;
                })
                .mutate(parent -> parent.setName("Changed"))
                .execute();

        Assert.assertEquals(p1.getMyId(), lookupDao.get("0").get().getMyId());
        Assert.assertEquals("Changed", lookupDao.get("0").get().getName());
        System.out.println(relationDao.get("0", 1L).get());
        Assert.assertEquals("Hello Changed",relationDao.get("0", 1L).get().getValue());
    }

    @Test
    public void testUpdateByCriteria() throws Exception {
        final String p1Id = "0";
        SomeLookupObject p1 = SomeLookupObject.builder()
                .myId(p1Id)
                .name("Parent 1")
                .build();

        SomeOtherObject c1 = relationDao.save(p1.getMyId(), SomeOtherObject.builder()
                .my_id(p1.getMyId())
                .value("Hello1")
                .build()).get();

        SomeOtherObject c2 = relationDao.save(p1.getMyId(), SomeOtherObject.builder()
                .my_id(p1.getMyId())
                .value("Hello2")
                .build()).get();

        final String p2Id = "1";
        SomeLookupObject p2 = SomeLookupObject.builder()
                .myId(p2Id)
                .name("Parent 2")
                .build();

        SomeOtherObject c3 = relationDao.save(p2.getMyId(), SomeOtherObject.builder()
                .my_id(p2.getMyId())
                .value("Hello3")
                .build()).get();

        lookupDao.save(p1);
        lookupDao.save(p2);

        final DetachedCriteria criteria = DetachedCriteria.forClass(SomeOtherObject.class)
                .add(Restrictions.eq("my_id", p1.getMyId()));

        final String childModifiedValue = "Hello Modified";
        final String parentModifiedValue = "Changed";
        lookupDao.lockAndGetExecutor(p1.getMyId())
                .updateAll(relationDao, criteria, 0, 5, entity -> {
                    entity.setValue(childModifiedValue);
                    return entity;
                })
                .mutate(parent -> parent.setName(parentModifiedValue))
                .execute();


        Assert.assertEquals(childModifiedValue,relationDao.get(p1.getMyId(), c1.getId()).get().getValue());
        Assert.assertEquals(childModifiedValue,relationDao.get(p1.getMyId(), c2.getId()).get().getValue());
        Assert.assertEquals(parentModifiedValue,lookupDao.get(p1Id).get().getName());
        Assert.assertEquals(p1Id,lookupDao.get(p1Id).get().getMyId());

        Assert.assertEquals("Hello3",relationDao.get(p2.getMyId(), c3.getId()).get().getValue());
        Assert.assertEquals(p2Id,lookupDao.get(p2Id).get().getMyId());
        Assert.assertEquals("Parent 2",lookupDao.get(p2Id).get().getName());
    }

    @Test
    public void testUpdateByEntity() throws Exception {
        SomeLookupObject p1 = SomeLookupObject.builder()
                .myId("0")
                .name("Parent 1")
                .build();

        SomeOtherObject c1 = relationDao.save(p1.getMyId(), SomeOtherObject.builder()
                .my_id(p1.getMyId())
                .value("Hello")
                .build()).get();


        lookupDao.saveAndGetExecutor(p1)
                .filter(parent -> !Strings.isNullOrEmpty(parent.getName()))
                .save(relationDao, c1, child -> {
                    child.setValue("Hello Changed");
                    return child;
                })
                .mutate(parent -> parent.setName("Changed"))
                .execute();

        Assert.assertEquals(p1.getMyId(), lookupDao.get("0").get().getMyId());
        Assert.assertEquals("Changed", lookupDao.get("0").get().getName());
        System.out.println(relationDao.get("0", 1L).get());
        Assert.assertEquals("Hello Changed",relationDao.get("0", 1L).get().getValue());
    }

    @Test(expected = ConstraintViolationException.class)
    public void testPersist_alreadyExistingDifferent() throws Exception {
        SomeLookupObject p1 = SomeLookupObject.builder()
                .myId("0")
                .name("Parent 1")
                .build();

        lookupDao.save(p1);

        SomeLookupObject p2 = SomeLookupObject.builder()
                .myId("0")
                .name("Changed")
                .build();

        lookupDao.saveAndGetExecutor(p2)
                .filter(parent -> !Strings.isNullOrEmpty(parent.getName()))
                .save(relationDao, parent -> SomeOtherObject.builder()
                        .my_id(parent.getMyId())
                        .value("Hello")
                        .build())
                .execute();

        Assert.assertEquals(p1.getMyId(), lookupDao.get("0").get().getMyId());
        Assert.assertEquals("Changed", lookupDao.get("0").get().getName());
    }

    @Test
    public void testPersist_alreadyExistingSame() throws Exception {
        SomeLookupObject p1 = SomeLookupObject.builder()
                .myId("0")
                .name("Parent 1")
                .build();

        lookupDao.save(p1);

        lookupDao.saveAndGetExecutor(p1)
                .filter(parent -> !Strings.isNullOrEmpty(parent.getName()))
                .save(relationDao, parent -> SomeOtherObject.builder()
                        .my_id(parent.getMyId())
                        .value("Hello")
                        .build())
                .mutate(parent -> parent.setName("Changed"))
                .execute();

        Assert.assertEquals(p1.getMyId(), lookupDao.get("0").get().getMyId());
        Assert.assertEquals("Changed", lookupDao.get("0").get().getName());
    }

    @Test
    public void testSelectAndUpdateOrSave() throws Exception {
        final String p1Id = "1";
        SomeLookupObject p1 = SomeLookupObject.builder()
                .myId(p1Id)
                .name("Parent 1")
                .build();
        lookupDao.save(p1);

        SomeOtherObject c1 = relationDao.save(p1.getMyId(), SomeOtherObject.builder()
                .my_id(p1.getMyId())
                .value("Hello")
                .build()).get();


        //test existing object
        final String childModifiedValue = "Hello Modified";
        final String parentModifiedValue = "Changed";
        final DetachedCriteria criteria1 = DetachedCriteria.forClass(SomeOtherObject.class)
                .add(Restrictions.eq("my_id", p1.getMyId()));

        lookupDao.lockAndGetExecutor(p1.getMyId())
                .selectAndUpdateOrSave(relationDao, criteria1, child -> {
                    child.setValue("abcd");
                    return child;
                })
                .mutate(parent -> parent.setName("xyzv"))
                .selectAndUpdateOrSave(relationDao, criteria1, child -> {
                    child.setValue(childModifiedValue);
                    return child;
                })
                .mutate(parent -> parent.setName(parentModifiedValue))
                .execute();

        Assert.assertEquals(childModifiedValue,relationDao.get(p1.getMyId(), c1.getId()).get().getValue());
        Assert.assertEquals(parentModifiedValue,lookupDao.get(p1Id).get().getName());
        Assert.assertEquals(p1Id,lookupDao.get(p1Id).get().getMyId());

        //test non existing object
        final String newChildValue = "Newly created child";
        final String newParentValue = "New parent Value";
        final DetachedCriteria criteria2 = DetachedCriteria.forClass(SomeOtherObject.class)
                .add(Restrictions.eq("value", newChildValue));
        lookupDao.lockAndGetExecutor(p1.getMyId())
                .selectAndUpdateOrSave(relationDao, criteria2, child -> {
                    Assert.assertEquals(null, child);
                    return SomeOtherObject.builder()
                            .my_id(p1.getMyId())
                            .value(newChildValue)
                            .build();
                })
                .mutate(parent -> parent.setName(newParentValue))
                .execute();

        final SomeOtherObject c2 = relationDao.select(p1.getMyId(), criteria2, 0, 1).stream().findFirst().get();
        Assert.assertEquals(newChildValue, c2.getValue());
        Assert.assertNotEquals(c1.getId(), c2.getId());
        Assert.assertEquals(newParentValue,lookupDao.get(p1Id).get().getName());
        Assert.assertEquals(p1Id,lookupDao.get(p1Id).get().getMyId());
    }
}
