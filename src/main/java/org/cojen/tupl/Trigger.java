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

import java.util.Arrays;

/**
 * Triggers are invoked immediately before modifications are made, for making observations and
 * for also making additional modifications. Triggers are only invoked when transactional
 * modifications are made, except when using the {@link LockMode#UNSAFE unsafe} lock
 * mode. Although triggers can observe the values being stored, they shouldn't directly alter
 * them.
 *
 * <p>If the trigger must be invoked during recovery and replication, add it with a {@link
 * DatabaseConfig#indexOpenListener listener}. This ensures that no modifcations are missed.
 *
 * @see View#addTrigger View.addTrigger
 * @author Brian S O'Neill
 */
public interface Trigger {
    /**
     * Invoked immediately before a {@link Cursor#store store} or {@link Cursor#commit commit}
     * operation.
     *
     * @param cursor positioned cursor, which references the original value (possibly {@link
     * Cursor#NOT_LOADED not loaded}).
     * @param value uncopied reference to the value being stored, which is null for a delete
     */
    public void store(Cursor cursor, byte[] value) throws IOException;

    /**
     * Invoked immediately before a {@link ValueAccessor#valueLength(long) valueLength}
     * operation. The default implementation of this method loads the value and calls the store
     * method.
     *
     * @param cursor positioned cursor, which references the original value (possibly {@link
     * Cursor#NOT_LOADED not loaded}).
     * @param length new value length, always greater than zero
     */
    public default void valueLength(Cursor cursor, long length) throws IOException {
        byte[] newValue;

        if (length == 0) {
            newValue = Utils.EMPTY_BYTES;
        } else {
            if (length > Integer.MAX_VALUE) {
                throw new LargeValueException(length);
            }
            newValue = new byte[(int) length];
            byte[] value = cursor.value();
            if (value == Cursor.NOT_LOADED) {
                cursor.load();
                value = cursor.value();
            }
            if (value != null) {
                System.arraycopy(value, 0, newValue, 0, Math.min(value.length, newValue.length));
            }
        }

        store(cursor, newValue);
    }

    /**
     * Invoked immediately before a {@link ValueAccessor#valueWrite valueWrite} operation The
     * default implementation of this method loads the value and calls the store method.
     *
     * @param cursor positioned cursor, which references the original value (possibly {@link
     * Cursor#NOT_LOADED not loaded}).
     * @param buf buffer to write from
     * @param off buffer start offset
     * @param len amount to write
     */
    public default void valueWrite(Cursor cursor, long pos, byte[] buf, int off, int len)
        throws IOException
    {
        byte[] newValue = ViewUtils.loadAndCopyValue(cursor, pos + len);
        System.arraycopy(buf, off, newValue, (int) pos, len);
        store(cursor, newValue);
    }

    /**
     * Invoked immediately before a {@link ValueAccessor#valueClear valueClear} operation. The
     * default implementation of this method loads the value and calls the store method.
     *
     * @param cursor positioned cursor, which references the original value (possibly {@link
     * Cursor#NOT_LOADED not loaded}).
     * @param pos start position to clear from
     * @param length amount to clear
     */
    public default void valueClear(Cursor cursor, long pos, long length) throws IOException {
        byte[] newValue = ViewUtils.loadAndCopyValue(cursor, pos + length);
        Arrays.fill(newValue, (int) pos, (int) (pos + length), (byte) 0);
        store(cursor, newValue);
    }
}
