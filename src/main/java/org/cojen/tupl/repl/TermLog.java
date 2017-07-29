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

package org.cojen.tupl.repl;

import java.io.Closeable;
import java.io.IOException;

/**
 * Log of data for a single term.
 *
 * @author Brian S O'Neill
 */
interface TermLog extends LKey<TermLog>, Closeable {
    @Override
    default long key() {
        return startIndex();
    }

    /**
     * Returns the fixed previous term of this log.
     */
    long prevTerm();

    /**
     * Returns the fixed term this log applies to.
     */
    long term();

    /**
     * Returns the fixed index at the start of the term.
     */
    long startIndex();

    /**
     * Returns the index at the end of the term (exclusive), which is Long.MAX_VALUE if undefined.
     */
    long endIndex();

    /**
     * Copies into all relevant fields of the given info object, for the highest index.
     */
    void captureHighest(LogInfo info);

    /**
     * Permit the commit index to advance. If the highest index (over a contiguous range)
     * is less than the given commit index, the actual commit index doesn't advance until
     * the highest index catches up.
     */
    void commit(long commitIndex);

    /**
     * Blocks until the commit index reaches the given index.
     *
     * @param nanosTimeout relative nanosecond time to wait; infinite if &lt;0
     * @return current commit index, or -1 if timed out or if term finished before the index
     * could be reached, or MIN_VALUE if closed
     */
    long waitForCommit(long index, long nanosTimeout) throws IOException;

    /**
     * Invokes the given task when the commit index reaches the requested index. The current
     * commit index is passed to the task, or -1 if the term ended before the index could be
     * reached, or MIN_VALUE if closed. If the commit index is high enough when this method is
     * called, then the current thread invokes the task.
     */
    void uponCommit(Delayed task);

    /**
     * Set the end index for this term instance, truncating all higher data. The highest index
     * will also be set, if the given index is within the contiguous region of data.
     *
     * @throws IllegalArgumentException if the given index is lower than the commit index
     * @throws IllegalStateException if the term is already finished at a lower index
     */
    void finishTerm(long endIndex) throws IOException;

    /**
     * Check for missing data by examination of the contiguous range. Pass in the highest
     * contiguous index (exclusive), as returned by the previous invocation of this method, or
     * pass Long.MAX_VALUE if unknown. The given callback receives all the missing ranges, and
     * an updated contiguous index is returned. As long as the contiguous range is changing, no
     * missing ranges are reported.
     */
    long checkForMissingData(long contigIndex, IndexRange results);

    /**
     * Returns a new or existing writer which can write data to the log, starting from the
     * given index.
     *
     * @param index any index in the term
     */
    LogWriter openWriter(long index) throws IOException;

    /**
     * Returns a new or existing reader which accesses data starting from the given index. The
     * reader returns EOF whenever the end of this term is reached.
     */
    LogReader openReader(long index) throws IOException;

    /**
     * Durably persist all data up to the highest index.
     */
    void sync() throws IOException;
}
