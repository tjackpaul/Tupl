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

/**
 * 
 *
 * @author Brian S O'Neill
 */
abstract class WrappedTrigger implements Trigger {
    protected final Trigger mSource;

    WrappedTrigger(Trigger source) {
        mSource = source;
    }

    @Override
    public void store(Cursor cursor, byte[] value) throws IOException {
        mSource.store(wrap(cursor), value);
    }

    @Override
    public void valueLength(Cursor cursor, long length) throws IOException {
        mSource.valueLength(wrap(cursor), length);
    }

    @Override
    public void valueWrite(Cursor cursor, long pos, byte[] buf, int off, int len)
        throws IOException
    {
        mSource.valueWrite(wrap(cursor), pos, buf, off, len);
    }

    @Override
    public void valueClear(Cursor cursor, long pos, long length) throws IOException {
        mSource.valueClear(wrap(cursor), pos, length);
    }

    protected abstract Cursor wrap(Cursor cursor);
}
