/*
 *  Copyright (C) 2017 Cojen.org
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl;

import java.io.IOException;
import java.util.*;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TriggerTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(TriggerTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        mDb = newTempDatabase(getClass());
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
        mDb = null;
    }

    protected Database mDb;

    @Test
    public void basicCursorStoreOps() throws Exception {
        Index ix = mDb.openIndex("test");
        Observer obs = new Observer();
        Object tkey = ix.addTrigger(obs);

        byte[] k1 = "k1".getBytes();
        byte[] v1 = "v1".getBytes();
        byte[] v2 = "v2".getBytes();

        // Test various auto-commit forms.

        Cursor c = ix.newCursor(null);
        c.find(k1);

        c.store(v1);
        obs.verifyOneAndClear(k1, null, v1);

        c.store(v2);
        obs.verifyOneAndClear(k1, v1, v2);

        ix.removeTrigger(tkey);
        c.store(v2);
        assertTrue(obs.observed.isEmpty());

        c.reset();

        Index temp = mDb.newTemporaryIndex();
        Observer tempObs = new Observer();
        temp.addTrigger(tempObs);

        c = temp.newCursor(null);
        c.find(k1);

        c.store(v1);
        assertNotNull(tempObs.txn);
        tempObs.verifyOneAndClear(k1, null, v1);

        c.reset();

        // Test with an explicit transaction.

        tkey = ix.addTrigger(obs);

        Transaction txn = mDb.newTransaction();
        c = ix.newCursor(txn);
        c.find(k1);

        c.store(v1);
        assertEquals(txn, obs.txn);
        obs.verifyOneAndClear(k1, v2, v1);

        Cursor tempCursor = temp.newCursor(txn);
        tempCursor.find(k1);
        tempCursor.store(v2);
        assertEquals(txn, tempObs.txn);
        tempObs.verifyOneAndClear(k1, v1, v2);

        c.commit(v2);
        assertEquals(txn, obs.txn);
        obs.verifyOneAndClear(k1, v1, v2);

        tempCursor.reset();
        c.reset();

        // Trigger doesn't get called for transactions which don't acquire locks.
        c = ix.newCursor(Transaction.BOGUS);
        c.find(k1);
        c.commit(v1);
        assertEquals(0, obs.observed.size());

        txn = mDb.newTransaction();
        tempCursor = temp.newCursor(txn);
        tempCursor.find(k1);
        tempCursor.commit(v1);
        assertEquals(txn, tempObs.txn);
        tempObs.verifyOneAndClear(k1, v2, v1);
    }

    @Test
    public void basicIndexStoreOps() throws Exception {
        Index ix = mDb.openIndex("test");
        Observer obs = new Observer(true);
        Object tkey = ix.addTrigger(obs);

        byte[] k1 = "k1".getBytes();
        byte[] v1 = "v1".getBytes();
        byte[] v2 = "v2".getBytes();

        // Test various auto-commit forms.

        ix.store(null, k1, v1);
        obs.verifyOneAndClear(k1, null, v1);

        ix.store(null, k1, v2);
        obs.verifyOneAndClear(k1, v1, v2);

        byte[] oldValue = ix.exchange(null, k1, v1);
        fastAssertArrayEquals(v2, oldValue);
        obs.verifyOneAndClear(k1, v2, v1);

        ix.removeTrigger(tkey);
        ix.store(null, k1, v2);
        assertTrue(obs.observed.isEmpty());

        // Test with an explicit transaction.

        tkey = ix.addTrigger(obs);

        Transaction txn = mDb.newTransaction();
        ix.store(txn, k1, v1);
        assertEquals(txn, obs.txn);
        obs.verifyOneAndClear(k1, v2, v1);
        oldValue = ix.exchange(txn, k1, v2);
        fastAssertArrayEquals(v1, oldValue);
        obs.verifyOneAndClear(k1, v1, v2);
        txn.reset();

        // Trigger doesn't get called for transactions which don't acquire locks.
        ix.store(Transaction.BOGUS, k1, v2);
        assertEquals(0, obs.observed.size());
        ix.exchange(Transaction.BOGUS, k1, v1);
        assertEquals(0, obs.observed.size());

        // Test insert and replace.

        assertFalse(ix.insert(null, k1, v1));
        assertEquals(0, obs.observed.size());
        assertTrue(ix.replace(null, k1, v2));
        obs.verifyOneAndClear(k1, v1, v2);
        assertTrue(ix.delete(null, k1));
        obs.verifyOneAndClear(k1, v2, null);
        assertFalse(ix.replace(null, k1, v1));
        assertEquals(0, obs.observed.size());
        assertTrue(ix.insert(null, k1, v1));
        obs.verifyOneAndClear(k1, null, v1);

        // Test both update variants.

        assertFalse(ix.update(null, k1, v1));
        assertEquals(0, obs.observed.size());
        assertTrue(ix.update(null, k1, v2));
        obs.verifyOneAndClear(k1, v1, v2);
        assertFalse(ix.update(null, k1, v1, v2));
        assertEquals(0, obs.observed.size());
        assertTrue(ix.update(null, k1, v2, v1));
        obs.verifyOneAndClear(k1, v2, v1);
    }

    @Test
    public void basicTriggerChain() throws Exception {
        Index ix = mDb.openIndex("test");
        Observer obs1 = new Observer();
        ix.addTrigger(obs1);
        Observer obs2 = new Observer();
        ix.addTrigger(obs2);

        byte[] k1 = "k1".getBytes();
        byte[] v1 = "v1".getBytes();
        byte[] v2 = "v2".getBytes();

        Cursor c = ix.newCursor(null);
        c.find(k1);
        c.store(v1);
        obs1.verifyOneAndClear(k1, null, v1);
        obs2.verifyOneAndClear(k1, null, v1);

        // Confirm LIFO trigger order.
        assertEquals(1, obs1.localCounter - obs2.localCounter);
    }

    @Test
    public void openAddTrigger() throws Exception {
        Map<String, Observer> observers = new HashMap<>();

        DatabaseConfig config = new DatabaseConfig()
            .directPageAccess(false)
            .indexOpenListener((db, ix) -> {
                Observer obs = new Observer();
                ix.addTrigger(obs);
                String name = ix.getNameString();
                observers.put(name, obs);

                if ("cycle".equals(name)) {
                    try {
                        Index self = db.openIndex(name);
                    } catch (Exception e) {
                        throw Utils.rethrow(e);
                    }
                }
            });

        Database db = newTempDatabase(getClass(), config);

        Index ix1 = db.openIndex("test1");
        Index ix2 = db.openIndex("test2");

        Observer obs1 = observers.get("test1");
        Observer obs2 = observers.get("test2");

        assertNotNull(obs1);
        assertNotNull(obs2);
        assertEquals(2, observers.size());

        byte[] key = "hello".getBytes();
        byte[] value = "world".getBytes();

        for (Index ix : new Index[] {ix1, ix2}) {
            Cursor c = ix.newCursor(null);
            c.find(key);
            c.store(value);
            c.reset();
        }

        obs1.verifyOneAndClear(key, null, value);
        obs2.verifyOneAndClear(key, null, value);

        observers.clear();
        ix1.close();
        ix2.close();

        ix1 = db.openIndex("test1");
        Observer obs = observers.get("test1");
        assertNotNull(obs);
        assertNotEquals(obs, obs1);
        assertEquals(1, observers.size());

        // Open an index which cycles back and opens itself.
        try {
            Index cycle = db.openIndex("cycle");
            fail();
        } catch (LockFailureException e) {
            // Deadlock averted.
        }
    }

    @Test
    public void reverseView() throws IOException {
        // Verify that the cursor passed to the trigger iterates in reverse.

        Index ix = mDb.openIndex("test");
        View view = ix.viewReverse();

        int[] countRef = new int[1];

        Object tkey = view.addTrigger((cursor, value) -> {
            countRef[0]++;
            assertNotNull(cursor.link());

            byte[] key = cursor.key();
            Cursor copy = cursor.copy();
            copy.next();
            if (copy.key() != null) {
                assertTrue(Utils.compareUnsigned(key, copy.key()) > 0);
            }

            copy.reset();
        });

        for (int i=0; i<3; i++) {
            byte[] key = ("key-" + i).getBytes();
            view.store(null, key, key);
        }

        assertEquals(3, countRef[0]);

        view.removeTrigger(tkey);

        try {
            view.removeTrigger(tkey);
            fail();
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void boundedView() throws IOException {
        // Verify that the trigger is called for in-range keys, and that the cursor passed to
        // the trigger is also bounded.

        Index ix = mDb.openIndex("test");
        View view = ix.viewGe("key-3".getBytes()).viewLt("key-8".getBytes());

        int[] countRef = new int[1];

        Object tkey = view.addTrigger((cursor, value) -> {
            countRef[0]++;
            assertNotNull(cursor.link());

            String key = new String(cursor.key());
            assertTrue("key-3".compareTo(key) <= 0);
            assertTrue("key-8".compareTo(key) > 0);

            Cursor copy = cursor.copy();
            copy.first();

            if ("key-3".equals(key)) {
                // First not stored yet.
                assertNull(copy.key());
            } else {
                assertEquals("key-3", new String(copy.key()));
            }

            copy.reset();
        });

        for (int i=0; i<9; i++) {
            byte[] key = ("key-" + i).getBytes();
            ix.store(null, key, key);
        }

        assertEquals(5, countRef[0]);

        view.removeTrigger(tkey);

        try {
            view.removeTrigger(tkey);
            fail();
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void transformedView() throws Exception {
        // Verify that the trigger is called for transformed views, and that the cursor is also
        // transformed.

        Index ix = mDb.openIndex("test");

        View view = ix.viewTransformed(new Transformer() {
            @Override
            public byte[] transformValue(byte[] value, byte[] key, byte[] tkey) {
                // Append "!" to end of value.
                if (value != null) {
                    value = Arrays.copyOfRange(value, 0, value.length + 1);
                    value[value.length - 1] = '!';
                }
                return value;
            }

            @Override
            public byte[] transformKey(Cursor cursor) {
                // Key must start with 'k'
                byte[] key = cursor.key();
                if (key.length > 0 && key[0] == 'k') {
                    return key;
                }
                return null;
            }
        });

        int[] countRef = new int[1];

        Object tkey = view.addTrigger((cursor, value) -> {
            countRef[0]++;
            assertNotNull(cursor.link());

            assertEquals('k', cursor.key()[0]);
            assertEquals('!', value[value.length - 1]);

            if (cursor.value() != null) {
                assertTrue(cursor.value() == Cursor.NOT_LOADED);
                cursor.load();
                assertEquals("world!", new String(cursor.value()));
            }

            Cursor copy = cursor.copy();
            copy.first();

            if (countRef[0] == 1) {
                // First not stored yet.
                assertNull(copy.key());
            } else {
                assertEquals("key-1", new String(copy.key()));
            }

            copy.reset();
        });

        ix.store(null, "hello".getBytes(), "world".getBytes());
        assertEquals(0, countRef[0]);

        ix.store(null, "key-1".getBytes(), "world".getBytes());
        assertEquals(1, countRef[0]);

        ix.store(null, "key-2".getBytes(), "world".getBytes());
        assertEquals(2, countRef[0]);

        // With autoload off...
        Cursor c = ix.newCursor(null);
        c.autoload(false);
        c.find("key-2".getBytes());
        c.store("value".getBytes());
        c.reset();
        assertEquals(3, countRef[0]);

        view.removeTrigger(tkey);

        try {
            view.removeTrigger(tkey);
            fail();
        } catch (IllegalStateException e) {
        }
    }

    static class Observed {
        byte[] key;
        LockResult lockResult;
        byte[] oldValue;
        byte[] newValue;

        void verify(byte[] key, byte[] oldValue, byte[] newValue) {
            fastAssertArrayEquals(key, this.key);
            fastAssertArrayEquals(oldValue, this.oldValue);
            assertTrue(newValue == this.newValue);
        }
    }

    static class Observer implements Trigger {
        final List<Observed> observed = new ArrayList<>();

        Transaction txn;

        int localCounter;
        private static int globalCounter;

        private final boolean mLoad;

        Observer() {
            this(false);
        }

        Observer(boolean load) {
            mLoad = load;
        }

        @Override
        public void store(Cursor cursor, byte[] value) throws IOException {
            localCounter = ++globalCounter;
            txn = cursor.link();

            Observed obs = new Observed();
            obs.key = cursor.key();

            obs.oldValue = cursor.value();
            if (obs.oldValue == Cursor.NOT_LOADED && mLoad) {
                cursor.load();
                obs.oldValue = cursor.value();
            }
            obs.newValue = value;
            observed.add(obs);
        }

        void verifyOneAndClear(byte[] key, byte[] oldValue, byte[] newValue) {
            assertEquals(1, observed.size());
            Observed obs = observed.get(0);
            obs.verify(key, oldValue, newValue);
            if (obs.lockResult != null) {
                assertEquals(LockResult.OWNED_EXCLUSIVE, obs.lockResult);
            }
            observed.clear();
            txn = null;
        }
    }
}
