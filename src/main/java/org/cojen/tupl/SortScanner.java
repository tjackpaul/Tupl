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
 * Scanner implementation intended for scanning and deleting sort results. No other threads
 * should be accessing the source temporary tree, which is deleted when the scan is complete,
 * or when the scanner is closed.
 *
 * @author Brian S O'Neill
 */
/*P*/
class SortScanner implements Scanner {
    private final LocalDatabase mDatabase;
    private TreeCursor mCursor;
    private Supplier mSupplier;

    /**
     * Must call ready or notReady to complete initialization.
     */
    SortScanner(LocalDatabase db) {
        mDatabase = db;
    }

    @Override
    public Comparator<byte[]> getComparator() {
        return cursor().getComparator();
    }

    @Override
    public byte[] key() {
        return cursor().key();
    }

    @Override
    public byte[] value() {
        return cursor().value();
    }

    @Override
    public boolean step() throws IOException {
        TreeCursor c = cursor();
        try {
            doStep(c);
            if (c.key() != null) {
                return true;
            }
            mDatabase.quickDeleteTemporaryTree(c.mTree);
            return false;
        } catch (UnpositionedCursorException e) {
            return false;
        } catch (Throwable e) {
            throw ViewUtils.fail(this, e);
        }
    }

    protected void doStep(TreeCursor c) throws IOException {
        c.deleteNext();
    }

    @Override
    public void close() throws IOException {
        try {
            TreeCursor c = mCursor;
            if (c != null) {
                mCursor = null;
                mDatabase.deleteIndex(c.mTree).run();
            } else if (mSupplier != null) {
                mSupplier.close();
                mSupplier = null;
            }
        } catch (ClosedIndexException e) {
            // Ignore potential double delete.
        } catch (IOException e) {
            if (!mDatabase.isClosed()) {
                throw e;
            }
        }
    }

    void ready(Tree tree) throws IOException {
        TreeCursor c = new TreeCursor(tree, Transaction.BOGUS);
        initPosition(c);
        mCursor = c;
    }

    protected void initPosition(TreeCursor c) throws IOException {
        c.first();
    }

    static interface Supplier {
        Tree get() throws IOException;

        void close() throws IOException;
    }

    void notReady(Supplier supplier) {
        mSupplier = supplier;
    }

    private TreeCursor cursor() {
        TreeCursor c = mCursor;
        return c == null ? openCursor() : c;
    }

    private TreeCursor openCursor() {
        try {
            Tree tree;
            if (mSupplier == null) {
                // Assume that scanner is being used after being closed.
                tree = mDatabase.newTemporaryIndex();
            } else {
                tree = mSupplier.get();
                mSupplier = null;
            }
            ready(tree);
            return mCursor;
        } catch (IOException e) {
            throw Utils.rethrow(e);
        }
    }
}
