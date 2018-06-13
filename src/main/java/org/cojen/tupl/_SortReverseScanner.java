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

import java.util.Comparator;

/**
 * Overrides inherited methods to scan in reverse order.
 *
 * @author Generated by PageAccessTransformer from SortReverseScanner.java
 */
/*P*/
class _SortReverseScanner extends _SortScanner {
    _SortReverseScanner(_LocalDatabase db) {
        super(db);
    }

    @Override
    public Comparator<byte[]> getComparator() {
        return super.getComparator().reversed();
    }

    @Override
    protected void doStep(_TreeCursor c) throws IOException {
        c.deletePrevious();
    }

    @Override
    protected void initPosition(_TreeCursor c) throws IOException {
        c.last();
    }
}