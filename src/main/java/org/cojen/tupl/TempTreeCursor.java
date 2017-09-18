/*
 *  Copyright (C) 2011-2017 Cojen.org
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
 * TreeCursor which prohibits redo durabilty.
 *
 * @author Brian S O'Neill
 */
/*P*/
final class TempTreeCursor extends TreeCursor {
    TempTreeCursor(TempTree tree, Transaction txn) {
        super(tree, txn);
    }

    TempTreeCursor(TempTree tree) {
        super(tree);
    }

    @Override
    protected int storeMode() {
        // Never redo.
        return 2;
    }

    @Override
    public void transferTo(Cursor target) throws IOException {
        Transaction txn = target.link();
        if (txn == null || txn != mTxn) {
            throw new IllegalArgumentException();
        }

        final DurabilityMode dmode = txn.durabilityMode();
        if (dmode == DurabilityMode.NO_REDO) {
            super.transferTo(target);
        } else {
            txn.durabilityMode(DurabilityMode.NO_REDO);
            try {
                super.transferTo(target);
            } finally {
                txn.durabilityMode(dmode);
            }
        }
    }
}
