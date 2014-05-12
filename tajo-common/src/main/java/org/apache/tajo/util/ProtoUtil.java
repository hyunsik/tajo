/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.util;

import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.util.Collection;

import static org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.*;

public class ProtoUtil {
  public static final BoolProto TRUE = BoolProto.newBuilder().setValue(true).build();
  public static final BoolProto FALSE = BoolProto.newBuilder().setValue(false).build();

  public static final NullProto NULL_PROTO = NullProto.newBuilder().build();

  public static StringProto convertString(String value) {
    return StringProto.newBuilder().setValue(value).build();
  }

  public static StringListProto convertStrings(Collection<String> strings) {
    return StringListProto.newBuilder().addAllValues(strings).build();
  }

  public static Collection<String> convertStrings(StringListProto strings) {
    return strings.getValuesList();
  }

  /**
   * Compute the number of bytes that would be needed to encode a varint.
   * {@code value} is treated as unsigned, so it won't be sign-extended if
   * negative.
   */
  public static int computeRawVarint32Size(final int value) {
    if ((value & (0xffffffff <<  7)) == 0) return 1;
    if ((value & (0xffffffff << 14)) == 0) return 2;
    if ((value & (0xffffffff << 21)) == 0) return 3;
    if ((value & (0xffffffff << 28)) == 0) return 4;
    return 5;
  }

  /**
   * Encode and write a varint.  {@code value} is treated as
   * unsigned, so it won't be sign-extended if negative.
   */
  public static void writeRawVarint32(int value, byte [] bytes, int srcPos) {
    int i = srcPos;
    while (true) {
      if ((value & ~0x7F) == 0) {
        bytes[i++] = (byte)value;
        return;
      } else {
        bytes[i] = (byte) ((value & 0x7F) | 0x80);
        value >>>= 7;
      }
    }
  }

  /**
   * Read a raw Varint from the stream.  If larger than 32 bits, discard the
   * upper bits.
   */
  public static int readRawVarint32(final byte [] bytes, final int srcPos) {
    int index = srcPos;

    byte tmp = bytes[index++];
    if (tmp >= 0) {
      return tmp;
    }
    int result = tmp & 0x7f;
    if ((tmp = bytes[index++]) >= 0) {
      result |= tmp << 7;
    } else {
      result |= (tmp & 0x7f) << 7;
      if ((tmp = bytes[index++]) >= 0) {
        result |= tmp << 14;
      } else {
        result |= (tmp & 0x7f) << 14;
        if ((tmp = bytes[index++]) >= 0) {
          result |= tmp << 21;
        } else {
          result |= (tmp & 0x7f) << 21;
          result |= (tmp = bytes[index++]) << 28;
          if (tmp < 0) {
            // Discard upper 32 bits.
            for (int i = 0; i < 5; i++) {
              if (bytes[index++] >= 0) {
                return result;
              }
            }
            throw new NumberFormatException("readRawVarint32 encountered a malformed varint.");
          }
        }
      }
    }
    return result;
  }
}
