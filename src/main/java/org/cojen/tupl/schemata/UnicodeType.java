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
public final class UnicodeType extends Type {
    public static final short ENCODING_UTF_8 = 1;
    public static final short ENCODING_UTF_16 = 2;
    public static final short ENCODING_UTF_32 = 3;

    private static final long HASH_BASE = 6974491338298857255L;

    private final Type mBaseType;
    private final short mEncoding;

    /**
     * @param baseType NumericType or ArrayType of NumericType
     */
    UnicodeType(Schemata schemata, long typeId, Type baseType, short encoding) {
        super(schemata, typeId, baseType.getFlags());
        mBaseType = baseType;
        mEncoding = encoding;
    }

    /**
     * @return NumericType or ArrayType of NumericType
     */
    public Type getBaseType() {
        return mBaseType;
    }

    @Override
    public boolean isFixedLength() {
        return mBaseType.isFixedLength();
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
        b.append("UnicodeType");
        b.append(" {");
        appendCommon(b);

        b.append(", ");
        b.append("encoding=");

        switch (mEncoding) {
        case ENCODING_UTF_8:
            b.append("UTF-8");
            break;
        case ENCODING_UTF_16:
            b.append("UTF-16");
            break;
        case ENCODING_UTF_32:
            b.append("UTF-32");
            break;
        default:
            b.append(mEncoding & 0xffffffff);
            break;
        }

        b.append(", ");
        b.append("baseType=");
        mBaseType.appendTo(b);

        b.append('}');
    }

    static UnicodeType decode(Transaction txn, Schemata schemata, long typeId, byte[] value)
        throws IOException
    {
        if (value[0] != TYPE_PREFIX_UNICODE) {
            throw new IllegalArgumentException();
        }
        return new UnicodeType(schemata, typeId,
                               schemata.decodeType(txn, value, 1), // baseType
                               (short) Utils.decodeUnsignedShortBE(value, 9)); // encoding
    }

    @Override
    long computeHash() {
        return mixHash(HASH_BASE + mEncoding, mBaseType);
    }

    @Override
    byte[] encodeValue() {
        byte[] value = new byte[1 + 8 + 2];
        value[0] = TYPE_PREFIX_UNICODE;
        encodeType(value, 1, mBaseType);
        Utils.encodeShortBE(value, 9, mEncoding);
        return value;
    }

    @Override
    @SuppressWarnings("unchecked")
    <T extends Type> T equivalent(T type) {
        if (type instanceof UnicodeType) {
            UnicodeType other = (UnicodeType) type;
            if (equalTypeIds(mBaseType, other.mBaseType) && mEncoding == other.mEncoding) {
                return (T) this;
            }
        }
        return null;
    }
}
