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
public final class ArrayType extends Type {
    private static final long HASH_BASE = 1858514625934181638L;

    private final Type mElementType;
    private final long mMinLength;
    private final long mMaxRange;

    ArrayType(Schemata schemata, long typeId, short flags,
              Type elementType, long minLength, long maxRange)
    {
        super(schemata, typeId, flags);
        mElementType = elementType;
        mMinLength = minLength;
        mMaxRange = maxRange;
    }

    public Type getElementType() {
        return mElementType;
    }

    public long getMinLength() {
        return mMinLength;
    }

    public long getMaxRange() {
        return mMaxRange;
    }

    @Override
    public boolean isFixedLength() {
        return mMaxRange == 0 && mElementType != null && mElementType.isFixedLength();
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
        b.append("ArrayType");
        b.append(" {");
        appendCommon(b);
        b.append(", ");
        b.append("elementType=");
        appendType(b, mElementType);
        b.append(", ");
        b.append("minLength=");
        b.append(mMinLength);
        b.append(", ");
        b.append("maxRange=");
        b.append(mMaxRange);
        b.append('}');
    }

    static ArrayType decode(Transaction txn, Schemata schemata, long typeId, byte[] value)
        throws IOException
    {
        if (value[0] != TYPE_PREFIX_ARRAY) {
            throw new IllegalArgumentException();
        }
        return new ArrayType(schemata, typeId,
                             (short) Utils.decodeUnsignedShortBE(value, 1), // flags
                             schemata.decodeType(txn, value, 3), // elementType
                             Utils.decodeLongBE(value, 11),  // minLength
                             Utils.decodeLongBE(value, 19)); // maxRange
    }

    @Override
    long computeHash() {
        long hash = mixHash(HASH_BASE + mFlags, mElementType);
        hash = hash * 31 + mMinLength;
        hash = hash * 31 + mMaxRange;
        return hash;
    }

    @Override
    byte[] encodeValue() {
        byte[] value = new byte[1 + 2 + 8 + 8 + 8];
        value[0] = TYPE_PREFIX_ARRAY;
        Utils.encodeShortBE(value, 1, mFlags);
        encodeType(value, 3, mElementType);
        Utils.encodeLongBE(value, 11, mMinLength);
        Utils.encodeLongBE(value, 19, mMaxRange);
        return value;
    }

    @Override
    @SuppressWarnings("unchecked")
    <T extends Type> T equivalent(T type) {
        if (type instanceof ArrayType) {
            ArrayType other = (ArrayType) type;
            if (mFlags == other.mFlags &&
                equalTypeIds(mElementType, other.mElementType) &&
                mMinLength == other.mMinLength &&
                mMaxRange == other.mMaxRange)
            {
                return (T) this;
            }
        }
        return null;
    }
}
