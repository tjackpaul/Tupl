/*
 *  Copyright (C) 2011-2018 Cojen.org
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

package org.cojen.tupl.schemata;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.cojen.tupl.Transaction;

import org.cojen.tupl.io.Utils;

/**
 *
 *
 * @author Brian S O'Neill
 */
public class AssembledType extends Type {
    private static final long HASH_BASE = 3315411704127731845L;

    private final Type[] mElementTypes;

    AssembledType(Schemata schemata, long typeId, short flags, Type[] elementTypes) {
        super(schemata, typeId, flags);
        mElementTypes = elementTypes;
    }

    public Type[] getElementTypes() {
        Type[] types = mElementTypes;
        if (types != null && types.length != 0) {
            types = types.clone();
        }

        return types;
    }

    @Override
    public boolean isFixedLength() {
        Type[] types = mElementTypes;
        if (types != null) {
            for (Type t : types) {
                if (!t.isFixedLength()) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public int printData(StringBuilder b, byte[] data, int offset) {
        // FIXME
        throw null;
    }

    @Override
    public int printKey(StringBuilder b, byte[] data, int offset) {
        // FIXME
        throw null;
    }

    @Override
    public int parseData(ByteArrayOutputStream out, String str, int offset) {
        // FIXME
        throw null;
    }

    @Override
    public int parseKey(ByteArrayOutputStream out, String str, int offset) {
        // FIXME
        throw null;
    }

    @Override
    void appendTo(StringBuilder b) {
        b.append("AssembledType");
        b.append(" {");
        appendCommon(b);
        b.append(", ");
        b.append("elementTypes=");
        appendTypes(b, mElementTypes);
        b.append('}');
    }

    static AssembledType decode(Transaction txn, Schemata schemata, long typeId, byte[] value)
        throws IOException
    {
        if (value[0] != TYPE_PREFIX_ASSEMBLED) {
            throw new IllegalArgumentException();
        }
        return new AssembledType(schemata, typeId,
                                 (short) Utils.decodeUnsignedShortBE(value, 1), // flags
                                 schemata.decodeTypes(txn, value, 3)); // elementTypes
    }

    @Override
    long computeHash() {
        return mixHash(HASH_BASE + mFlags, mElementTypes);
    }

    @Override
    byte[] encodeValue() {
        byte[] value = new byte[1 + 2 + mElementTypes.length * 8];
        value[0] = TYPE_PREFIX_ASSEMBLED;
        Utils.encodeShortBE(value, 1, mFlags);
        int off = 3;
        for (Type t : mElementTypes) {
            encodeType(value, off, t);
            off += 8;
        }
        return value;
    }

    @Override
    @SuppressWarnings("unchecked")
    <T extends Type> T equivalent(T type) {
        if (type instanceof AssembledType) {
            AssembledType other = (AssembledType) type;
            if (mFlags == other.mFlags &&
                equalTypeIds(mElementTypes, other.mElementTypes))
            {
                return (T) this;
            }
        }
        return null;
    }
}
