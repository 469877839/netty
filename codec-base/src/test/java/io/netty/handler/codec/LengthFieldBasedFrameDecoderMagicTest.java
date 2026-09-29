/*
 * Copyright 2026 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * specific language governing permissions and limitations under the License.
 */
package io.netty.handler.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LengthFieldBasedFrameDecoderMagicTest {

    private static final int MAGIC = 0xCAFE;
    private static final int MAX_FRAME_LENGTH = 64;

    // Frame layout: magic (2 bytes) | length (2 bytes, body length) | body
    private static LengthFieldBasedFrameDecoder strictDecoder() {
        return new LengthFieldBasedFrameDecoder(
                ByteOrder.BIG_ENDIAN, MAX_FRAME_LENGTH, 2, 2, 0, 0, true, MAGIC, 0, 2);
    }

    private static LengthFieldBasedFrameDecoder resyncDecoder(boolean failFast, int maxResyncScanBytes) {
        return new LengthFieldBasedFrameDecoder(
                ByteOrder.BIG_ENDIAN, MAX_FRAME_LENGTH, 2, 2, 0, 0, failFast, MAGIC, 0, 2, maxResyncScanBytes);
    }

    private static byte[] frame(int... body) {
        ByteBuf buf = Unpooled.buffer();
        try {
            buf.writeShort(MAGIC);
            buf.writeShort(body.length);
            for (int b : body) {
                buf.writeByte(b);
            }
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            return bytes;
        } finally {
            buf.release();
        }
    }

    private static void assertFrame(EmbeddedChannel channel, byte[] expected) {
        ByteBuf frame = channel.readInbound();
        assertNotNull(frame);
        try {
            byte[] actual = new byte[frame.readableBytes()];
            frame.readBytes(actual);
            assertArrayEquals(expected, actual);
        } finally {
            frame.release();
        }
    }

    @Test
    public void testMagicValidationDecodesFrames() {
        EmbeddedChannel channel = new EmbeddedChannel(strictDecoder());
        byte[] frame1 = frame(1, 2, 3);
        byte[] frame2 = frame(4, 5);
        channel.writeInbound(Unpooled.wrappedBuffer(frame1, frame2));
        assertFrame(channel, frame1);
        assertFrame(channel, frame2);
        assertNull(channel.readInbound());
        channel.finishAndReleaseAll();
    }

    @Test
    public void testMagicMismatchStrictThrows() {
        EmbeddedChannel channel = new EmbeddedChannel(strictDecoder());
        ByteBuf garbage = Unpooled.wrappedBuffer(new byte[] { 'G', 'A', 'R', 'B', 'A', 'G', 'E' });
        assertThrows(CorruptedFrameException.class, () -> channel.writeInbound(garbage));
        // The rejected bytes are still pending, so the final decode attempt fails again.
        assertThrows(CorruptedFrameException.class, channel::finishAndReleaseAll);
    }

    @Test
    public void testMagicInsidePayloadIsNotConfused() {
        EmbeddedChannel channel = new EmbeddedChannel(strictDecoder());
        // The payload itself contains the magic number bytes.
        byte[] frame1 = frame((byte) 0xCA, (byte) 0xFE, (byte) 0xCA, (byte) 0xFE);
        byte[] frame2 = frame(9);
        channel.writeInbound(Unpooled.wrappedBuffer(frame1, frame2));
        assertFrame(channel, frame1);
        assertFrame(channel, frame2);
        assertNull(channel.readInbound());
        channel.finishAndReleaseAll();
    }

    @Test
    public void testPartialHeaderWaitsForMoreData() {
        EmbeddedChannel channel = new EmbeddedChannel(strictDecoder());
        byte[] frame = frame(7, 7);
        // Only the magic number so far: not enough to validate the header, must not fail or scan.
        channel.writeInbound(Unpooled.wrappedBuffer(frame, 0, 2));
        assertNull(channel.readInbound());
        channel.writeInbound(Unpooled.wrappedBuffer(frame, 2, frame.length - 2));
        assertFrame(channel, frame);
        assertNull(channel.readInbound());
        channel.finishAndReleaseAll();
    }

    @Test
    public void testResyncAfterGarbage() {
        EmbeddedChannel channel = new EmbeddedChannel(resyncDecoder(true, 16));
        byte[] frame = frame(1, 2, 3);
        ByteBuf in = Unpooled.buffer();
        in.writeBytes(new byte[] { 'G', 'A', 'R', 'B', 'A', 'G', 'E' });
        in.writeBytes(frame);
        channel.writeInbound(in);
        assertFrame(channel, frame);
        assertNull(channel.readInbound());
        channel.finishAndReleaseAll();
    }

    @Test
    public void testResyncAcrossReads() {
        EmbeddedChannel channel = new EmbeddedChannel(resyncDecoder(true, 16));
        // Feed garbage one byte at a time: the scan position must be remembered across decode calls.
        for (int i = 0; i < 5; i++) {
            channel.writeInbound(Unpooled.wrappedBuffer(new byte[] { (byte) ('A' + i) }));
            assertNull(channel.readInbound());
        }
        byte[] frame = frame(1, 2, 3, 4);
        // Feed the frame in two chunks.
        channel.writeInbound(Unpooled.wrappedBuffer(frame, 0, 3));
        assertNull(channel.readInbound());
        channel.writeInbound(Unpooled.wrappedBuffer(frame, 3, frame.length - 3));
        assertFrame(channel, frame);
        assertNull(channel.readInbound());
        channel.finishAndReleaseAll();
    }

    @Test
    public void testInterleavedGarbageAndFrames() {
        EmbeddedChannel channel = new EmbeddedChannel(resyncDecoder(true, 16));
        byte[] frame1 = frame(1);
        byte[] frame2 = frame(2, 2);
        ByteBuf in = Unpooled.buffer();
        in.writeBytes(new byte[] { 'X', 'Y' });
        in.writeBytes(frame1);
        in.writeBytes(new byte[] { 'Z', 'Z', 'Z' });
        in.writeBytes(frame2);
        channel.writeInbound(in);
        assertFrame(channel, frame1);
        assertFrame(channel, frame2);
        assertNull(channel.readInbound());
        channel.finishAndReleaseAll();
    }

    @Test
    public void testFalsePositiveMagicIsSuppressedByLengthCheck() {
        EmbeddedChannel channel = new EmbeddedChannel(resyncDecoder(false, 16));
        // Garbage that starts with the magic number but is followed by an implausible length field.
        ByteBuf in = Unpooled.buffer();
        in.writeBytes(new byte[] { (byte) 0xCA, (byte) 0xFE, 0x7F, (byte) 0xFF, 0x00, 0x01 });
        byte[] frame = frame(5, 6);
        in.writeBytes(frame);
        channel.writeInbound(in);
        assertFrame(channel, frame);
        assertNull(channel.readInbound());
        channel.finishAndReleaseAll();
    }

    @Test
    public void testResyncScanWindowExceeded() {
        EmbeddedChannel channel = new EmbeddedChannel(resyncDecoder(true, 8));
        ByteBuf garbage = Unpooled.wrappedBuffer(new byte[20]);
        assertThrows(CorruptedFrameException.class, () -> channel.writeInbound(garbage));
        channel.finishAndReleaseAll();
    }

    @Test
    public void testFailFastThrowsOnForgedLengthField() {
        // failFast=true: an implausible length field at a magic-validated position throws immediately.
        EmbeddedChannel channel = new EmbeddedChannel(resyncDecoder(true, 16));
        ByteBuf in = Unpooled.buffer();
        in.writeShort(MAGIC);
        in.writeShort(1000); // larger than maxFrameLength
        in.writeBytes(new byte[10]);
        assertThrows(TooLongFrameException.class, () -> channel.writeInbound(in));
        channel.finishAndReleaseAll();
    }

    @Test
    public void testNoFailFastResyncsOnForgedLengthField() {
        // failFast=false: the magic number match is treated as a false positive and the
        // decoder resynchronizes instead of throwing.
        EmbeddedChannel channel = new EmbeddedChannel(resyncDecoder(false, 16));
        ByteBuf in = Unpooled.buffer();
        in.writeShort(MAGIC);
        in.writeShort(1000); // larger than maxFrameLength
        in.writeBytes(new byte[] { 1, 2, 3 });
        byte[] frame = frame(8, 8, 8);
        in.writeBytes(frame);
        channel.writeInbound(in);
        assertFrame(channel, frame);
        assertNull(channel.readInbound());
        channel.finishAndReleaseAll();
    }

    @Test
    public void testTooLongFrameDiscardStillWorksWithMagic() {
        // failFast=false without resynchronization: the legacy discard-then-throw behavior.
        EmbeddedChannel channel = new EmbeddedChannel(new LengthFieldBasedFrameDecoder(
                ByteOrder.BIG_ENDIAN, MAX_FRAME_LENGTH, 2, 2, 0, 0, false, MAGIC, 0, 2));
        ByteBuf first = Unpooled.buffer();
        first.writeShort(MAGIC);
        first.writeShort(100); // larger than maxFrameLength, frame length is 104
        first.writeBytes(new byte[2]);
        // No exception yet: the frame is discarded first.
        channel.writeInbound(first);
        assertNull(channel.readInbound());
        // Exactly the remaining 98 bytes of the too-long frame: the exception is
        // thrown once everything was discarded.
        assertThrows(TooLongFrameException.class,
                () -> channel.writeInbound(Unpooled.wrappedBuffer(new byte[98])));
        byte[] frame = frame(1, 2);
        channel.writeInbound(Unpooled.wrappedBuffer(frame));
        assertFrame(channel, frame);
        assertNull(channel.readInbound());
        channel.finishAndReleaseAll();
    }

    @Test
    public void testNegativeLengthFieldKeepsLegacyException() {
        EmbeddedChannel channel = new EmbeddedChannel(new LengthFieldBasedFrameDecoder(
                ByteOrder.BIG_ENDIAN, MAX_FRAME_LENGTH, 2, 8, 0, 0, true, MAGIC, 0, 2));
        ByteBuf in = Unpooled.buffer();
        in.writeShort(MAGIC);
        in.writeLong(-1);
        CorruptedFrameException e =
                assertThrows(CorruptedFrameException.class, () -> channel.writeInbound(in));
        assertEquals("negative pre-adjustment length field: -1", e.getMessage());
        channel.finishAndReleaseAll();
    }

    @Test
    public void testLittleEndianMagicAndLength() {
        EmbeddedChannel channel = new EmbeddedChannel(new LengthFieldBasedFrameDecoder(
                ByteOrder.LITTLE_ENDIAN, MAX_FRAME_LENGTH, 2, 2, 0, 0, true, MAGIC, 0, 2, 16));
        ByteBuf in = Unpooled.buffer();
        in.writeBytes(new byte[] { 'G', 'G' }); // garbage
        in.writeShortLE(MAGIC);
        in.writeShortLE(2);
        in.writeBytes(new byte[] { 0x11, 0x22 });
        channel.writeInbound(in);
        ByteBuf frame = channel.readInbound();
        assertNotNull(frame);
        try {
            assertEquals(6, frame.readableBytes());
            assertEquals(MAGIC, frame.readUnsignedShortLE());
            assertEquals(2, frame.readUnsignedShortLE());
            assertEquals(0x11, frame.readByte());
            assertEquals(0x22, frame.readByte());
        } finally {
            frame.release();
        }
        assertNull(channel.readInbound());
        channel.finishAndReleaseAll();
    }

    @Test
    public void testInvalidMagicNumberLength() {
        assertThrows(IllegalArgumentException.class, () -> new LengthFieldBasedFrameDecoder(
                ByteOrder.BIG_ENDIAN, MAX_FRAME_LENGTH, 2, 2, 0, 0, true, MAGIC, 0, 5));
    }

    @Test
    public void testMagicNumberDoesNotFit() {
        assertThrows(IllegalArgumentException.class, () -> new LengthFieldBasedFrameDecoder(
                ByteOrder.BIG_ENDIAN, MAX_FRAME_LENGTH, 2, 2, 0, 0, true, 0x1CAFE, 0, 2));
    }

    @Test
    public void testMagicNumberOffsetOutOfRange() {
        assertThrows(IllegalArgumentException.class, () -> new LengthFieldBasedFrameDecoder(
                ByteOrder.BIG_ENDIAN, MAX_FRAME_LENGTH, 2, 2, 0, 0, true, MAGIC, 63, 2));
    }
}
