/*
 *  Copyright (C) 2018 Cojen.org
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

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class SorterReplicationTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(SorterReplicationTest.class.getName());
    }

    protected DatabaseConfig decorate(DatabaseConfig config) throws Exception {
        config.directPageAccess(false);
        return config;
    }

    @Before
    public void setup() throws Exception {
        mReplicaMan = new SocketReplicationManager(null, 0);
        mLeaderMan = new SocketReplicationManager("localhost", mReplicaMan.getPort());

        DatabaseConfig config = new DatabaseConfig()
            .minCacheSize(10_000_000)
            .maxCacheSize(100_000_000)
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .replicate(mLeaderMan);

        config = decorate(config);

        mLeader = TestUtils.newTempDatabase(getClass(), config);

        config.replicate(mReplicaMan);
        mReplica = newTempDatabase(getClass(), config);

        mLeaderMan.waitForLeadership();
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
        mReplicaMan = null;
        mLeaderMan = null;
        mReplica = null;
        mLeader = null;
    }

    private SocketReplicationManager mReplicaMan, mLeaderMan;
    private Database mReplica, mLeader;

    @Test
    public void sortNothing() throws Exception {
        Sorter s = mLeader.newSorter(null);

        Index ix1 = s.finish("test-1", null);
        fence();
        assertEquals(0, ix1.count(null, null));
        Index ix2 = mReplica.findIndex("test-1");
        assertNotNull(ix2);
        assertEquals(0, ix2.count(null, null));

        /* FIXME
        Index ix2 = s.finish();
        assertEquals(0, ix2.count(null, null));
        assertNotSame(ix1, ix2);

        s.reset();
        Index ix3 = s.finish();
        assertEquals(0, ix3.count(null, null));
        assertNotSame(ix1, ix3);
        assertNotSame(ix2, ix3);

        s.reset();
        s.reset();
        Index ix4 = s.finish();
        assertEquals(0, ix4.count(null, null));
        */
    }

    /**
     * Writes a fence to the leader and waits for the replica to catch up.
     */
    private void fence() throws IOException, InterruptedException {
        byte[] message = ("fence:" + System.nanoTime()).getBytes();
        long pos = mLeaderMan.writeControl(message);
        mReplicaMan.waitForControl(pos, message);
    }
}
