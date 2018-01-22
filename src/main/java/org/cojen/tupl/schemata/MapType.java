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
public class MapType extends Type {
    private static final long HASH_BASE = 7558341980033698328L;

    private final Type mKeyType;
    private final Type mValueType;

    MapType(Schemata schemata, long typeId, short flags, Type keyType, Type valueType) {
        super(schemata, typeId, flags);
        mKeyType = keyType;
        mValueType = valueType;
    }

    public Type getKeyType() {
        return mKeyType;
    }

    public Type getValueType() {
        return mValueType;
    }

    @Override
    public boolean isFixedLength() {
        return mKeyType != null && mKeyType.isFixedLength()
            && (mValueType == null || mValueType.isFixedLength());
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
        b.append("MapType");
        b.append(" {");
        appendCommon(b);
        b.append(", ");
        b.append("keyType=");
        appendType(b, mKeyType);
        b.append(", ");
        b.append("valueType=");
        appendType(b, mValueType);
        b.append('}');
    }

    static MapType decode(Transaction txn, Schemata schemata, long typeId, byte[] value)
        throws IOException
    {
        if (value[0] != TYPE_PREFIX_MAP) {
            throw new IllegalArgumentException();
        }
        return new MapType(schemata, typeId,
                           (short) Utils.decodeUnsignedShortBE(value, 1), // flags
                           schemata.decodeType(txn, value, 3),   // keyType
                           schemata.decodeType(txn, value, 11)); // valueType
    }

    @Override
    long computeHash() {
        long hash = mixHash(HASH_BASE + mFlags, mKeyType);
        hash = mixHash(hash, mValueType);
        return hash;
    }

    @Override
    byte[] encodeValue() {
        byte[] value = new byte[1 + 2 + 8 + 8];
        value[0] = TYPE_PREFIX_MAP;
        Utils.encodeShortBE(value, 1, mFlags);
        encodeType(value, 3, mKeyType);
        encodeType(value, 11, mValueType);
        return value;
    }

    @Override
    @SuppressWarnings("unchecked")
    <T extends Type> T equivalent(T type) {
        if (type instanceof MapType) {
            MapType other = (MapType) type;
            if (mFlags == other.mFlags &&
                equalTypeIds(mKeyType, other.mKeyType) &&
                equalTypeIds(mValueType, other.mValueType))
            {
                return (T) this;
            }
        }
        return null;
    }
}
