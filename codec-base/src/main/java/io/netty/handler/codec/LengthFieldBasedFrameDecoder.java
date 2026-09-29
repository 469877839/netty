/*
 * Copyright 2012 The Netty Project
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
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.handler.codec;

import static io.netty.util.internal.ObjectUtil.checkNotNull;
import static io.netty.util.internal.ObjectUtil.checkPositive;
import static io.netty.util.internal.ObjectUtil.checkPositiveOrZero;

import java.nio.ByteOrder;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * A decoder that splits the received {@link ByteBuf}s dynamically by the
 * value of the length field in the message.  It is particularly useful when you
 * decode a binary message which has an integer header field that represents the
 * length of the message body or the whole message.
 * <p>
 * {@link LengthFieldBasedFrameDecoder} has many configuration parameters so
 * that it can decode any message with a length field, which is often seen in
 * proprietary client-server protocols. Here are some example that will give
 * you the basic idea on which option does what.
 *
 * <h3>2 bytes length field at offset 0, do not strip header</h3>
 *
 * The value of the length field in this example is <tt>12 (0x0C)</tt> which
 * represents the length of "HELLO, WORLD".  By default, the decoder assumes
 * that the length field represents the number of the bytes that follows the
 * length field.  Therefore, it can be decoded with the simplistic parameter
 * combination.
 * <pre>
 * <b>lengthFieldOffset</b>   = <b>0</b>
 * <b>lengthFieldLength</b>   = <b>2</b>
 * lengthAdjustment    = 0
 * initialBytesToStrip = 0 (= do not strip header)
 *
 * BEFORE DECODE (14 bytes)         AFTER DECODE (14 bytes)
 * +--------+----------------+      +--------+----------------+
 * | Length | Actual Content |----->| Length | Actual Content |
 * | 0x000C | "HELLO, WORLD" |      | 0x000C | "HELLO, WORLD" |
 * +--------+----------------+      +--------+----------------+
 * </pre>
 *
 * <h3>2 bytes length field at offset 0, strip header</h3>
 *
 * Because we can get the length of the content by calling
 * {@link ByteBuf#readableBytes()}, you might want to strip the length
 * field by specifying <tt>initialBytesToStrip</tt>.  In this example, we
 * specified <tt>2</tt>, that is same with the length of the length field, to
 * strip the first two bytes.
 * <pre>
 * lengthFieldOffset   = 0
 * lengthFieldLength   = 2
 * lengthAdjustment    = 0
 * <b>initialBytesToStrip</b> = <b>2</b> (= the length of the Length field)
 *
 * BEFORE DECODE (14 bytes)         AFTER DECODE (12 bytes)
 * +--------+----------------+      +----------------+
 * | Length | Actual Content |----->| Actual Content |
 * | 0x000C | "HELLO, WORLD" |      | "HELLO, WORLD" |
 * +--------+----------------+      +----------------+
 * </pre>
 *
 * <h3>2 bytes length field at offset 0, do not strip header, the length field
 *     represents the length of the whole message</h3>
 *
 * In most cases, the length field represents the length of the message body
 * only, as shown in the previous examples.  However, in some protocols, the
 * length field represents the length of the whole message, including the
 * message header.  In such a case, we specify a non-zero
 * <tt>lengthAdjustment</tt>.  Because the length value in this example message
 * is always greater than the body length by <tt>2</tt>, we specify <tt>-2</tt>
 * as <tt>lengthAdjustment</tt> for compensation.
 * <pre>
 * lengthFieldOffset   =  0
 * lengthFieldLength   =  2
 * <b>lengthAdjustment</b>    = <b>-2</b> (= the length of the Length field)
 * initialBytesToStrip =  0
 *
 * BEFORE DECODE (14 bytes)         AFTER DECODE (14 bytes)
 * +--------+----------------+      +--------+----------------+
 * | Length | Actual Content |----->| Length | Actual Content |
 * | 0x000E | "HELLO, WORLD" |      | 0x000E | "HELLO, WORLD" |
 * +--------+----------------+      +--------+----------------+
 * </pre>
 *
 * <h3>3 bytes length field at the end of 5 bytes header, do not strip header</h3>
 *
 * The following message is a simple variation of the first example.  An extra
 * header value is prepended to the message.  <tt>lengthAdjustment</tt> is zero
 * again because the decoder always takes the length of the prepended data into
 * account during frame length calculation.
 * <pre>
 * <b>lengthFieldOffset</b>   = <b>2</b> (= the length of Header 1)
 * <b>lengthFieldLength</b>   = <b>3</b>
 * lengthAdjustment    = 0
 * initialBytesToStrip = 0
 *
 * BEFORE DECODE (17 bytes)                      AFTER DECODE (17 bytes)
 * +----------+----------+----------------+      +----------+----------+----------------+
 * | Header 1 |  Length  | Actual Content |----->| Header 1 |  Length  | Actual Content |
 * |  0xCAFE  | 0x00000C | "HELLO, WORLD" |      |  0xCAFE  | 0x00000C | "HELLO, WORLD" |
 * +----------+----------+----------------+      +----------+----------+----------------+
 * </pre>
 *
 * <h3>3 bytes length field at the beginning of 5 bytes header, do not strip header</h3>
 *
 * This is an advanced example that shows the case where there is an extra
 * header between the length field and the message body.  You have to specify a
 * positive <tt>lengthAdjustment</tt> so that the decoder counts the extra
 * header into the frame length calculation.
 * <pre>
 * lengthFieldOffset   = 0
 * lengthFieldLength   = 3
 * <b>lengthAdjustment</b>    = <b>2</b> (= the length of Header 1)
 * initialBytesToStrip = 0
 *
 * BEFORE DECODE (17 bytes)                      AFTER DECODE (17 bytes)
 * +----------+----------+----------------+      +----------+----------+----------------+
 * |  Length  | Header 1 | Actual Content |----->|  Length  | Header 1 | Actual Content |
 * | 0x00000C |  0xCAFE  | "HELLO, WORLD" |      | 0x00000C |  0xCAFE  | "HELLO, WORLD" |
 * +----------+----------+----------------+      +----------+----------+----------------+
 * </pre>
 *
 * <h3>2 bytes length field at offset 1 in the middle of 4 bytes header,
 *     strip the first header field and the length field</h3>
 *
 * This is a combination of all the examples above.  There are the prepended
 * header before the length field and the extra header after the length field.
 * The prepended header affects the <tt>lengthFieldOffset</tt> and the extra
 * header affects the <tt>lengthAdjustment</tt>.  We also specified a non-zero
 * <tt>initialBytesToStrip</tt> to strip the length field and the prepended
 * header from the frame.  If you don't want to strip the prepended header, you
 * could specify <tt>0</tt> for <tt>initialBytesToSkip</tt>.
 * <pre>
 * lengthFieldOffset   = 1 (= the length of HDR1)
 * lengthFieldLength   = 2
 * <b>lengthAdjustment</b>    = <b>1</b> (= the length of HDR2)
 * <b>initialBytesToStrip</b> = <b>3</b> (= the length of HDR1 + LEN)
 *
 * BEFORE DECODE (16 bytes)                       AFTER DECODE (13 bytes)
 * +------+--------+------+----------------+      +------+----------------+
 * | HDR1 | Length | HDR2 | Actual Content |----->| HDR2 | Actual Content |
 * | 0xCA | 0x000C | 0xFE | "HELLO, WORLD" |      | 0xFE | "HELLO, WORLD" |
 * +------+--------+------+----------------+      +------+----------------+
 * </pre>
 *
 * <h3>2 bytes length field at offset 1 in the middle of 4 bytes header,
 *     strip the first header field and the length field, the length field
 *     represents the length of the whole message</h3>
 *
 * Let's give another twist to the previous example.  The only difference from
 * the previous example is that the length field represents the length of the
 * whole message instead of the message body, just like the third example.
 * We have to count the length of HDR1 and Length into <tt>lengthAdjustment</tt>.
 * Please note that we don't need to take the length of HDR2 into account
 * because the length field already includes the whole header length.
 * <pre>
 * lengthFieldOffset   =  1
 * lengthFieldLength   =  2
 * <b>lengthAdjustment</b>    = <b>-3</b> (= the length of HDR1 + LEN, negative)
 * <b>initialBytesToStrip</b> = <b> 3</b>
 *
 * BEFORE DECODE (16 bytes)                       AFTER DECODE (13 bytes)
 * +------+--------+------+----------------+      +------+----------------+
 * | HDR1 | Length | HDR2 | Actual Content |----->| HDR2 | Actual Content |
 * | 0xCA | 0x0010 | 0xFE | "HELLO, WORLD" |      | 0xFE | "HELLO, WORLD" |
 * +------+--------+------+----------------+      +------+----------------+
 * </pre>
 *
 * <h3>Magic number validation and resynchronization</h3>
 *
 * In addition to the length field, the decoder can validate a magic number in the
 * frame header before it attempts to decode a frame.  The magic number is configured
 * with {@code magicNumber}, {@code magicNumberOffset} and {@code magicNumberLength}
 * (1, 2, 3, 4 or 8 bytes, decoded with the same {@link ByteOrder} as the length field).
 * When the bytes at the configured offset do not match the magic number, no frame is
 * produced for that position.
 * <p>
 * What happens next depends on whether resynchronization is enabled
 * ({@code maxResyncScanBytes} &gt; 0):
 * <ul>
 *     <li>Resynchronization disabled: a {@link CorruptedFrameException} is thrown
 *     immediately on the first magic number mismatch.</li>
 *     <li>Resynchronization enabled: the decoder scans the accumulated bytes forward for
 *     the next occurrence of the magic number and restarts frame detection there.  The
 *     bytes skipped while scanning are silently discarded.  To avoid mistaking magic
 *     number bytes that appear inside a length field or a payload for a frame boundary,
 *     a candidate position is only accepted when its length field is self-consistent
 *     (non-negative, adjusted frame length between the header length and
 *     {@code maxFrameLength}).  If no plausible magic number is found within
 *     {@code maxResyncScanBytes} bytes, a {@link CorruptedFrameException} is thrown.</li>
 * </ul>
 * <p>
 * Because {@code failFast} and resynchronization cannot both win, the following
 * deterministic rule applies when a length field looks untrustworthy (negative,
 * smaller than the header, or larger than {@code maxFrameLength}) at a position where
 * the magic number matched:
 * <ul>
 *     <li>{@code failFast == true}: the legacy behavior is kept and the decoder throws
 *     immediately ({@link TooLongFrameException} or {@link CorruptedFrameException},
 *     exactly as without magic number validation).</li>
 *     <li>{@code failFast == false}: the magic number match is assumed to be a false
 *     positive and the decoder resynchronizes instead of throwing.</li>
 * </ul>
 * A magic number mismatch always triggers resynchronization first, regardless of
 * {@code failFast}.  If the buffer does not contain enough bytes to validate the magic
 * number and the length field (e.g. only half a frame has been received), the decoder
 * waits for more data and neither scans nor fails.
 * <p>
 * The scan progress is tracked by consuming the scanned bytes from the cumulative buffer,
 * so it stays consistent across {@code decode} invocations and across compaction of the
 * cumulative buffer (see {@link ByteToMessageDecoder#discardSomeReadBytes()}), and no
 * per-frame copies or unreleased slices are created while scanning.
 *
 * @see LengthFieldPrepender
 */
public class LengthFieldBasedFrameDecoder extends ByteToMessageDecoder {

    private final ByteOrder byteOrder;
    private final int maxFrameLength;
    private final int lengthFieldOffset;
    private final int lengthFieldLength;
    private final int lengthFieldEndOffset;
    private final int lengthAdjustment;
    private final int initialBytesToStrip;
    private final boolean failFast;
    private final long magicNumber;
    private final int magicNumberOffset;
    private final int magicNumberLength;
    private final int magicNumberEndOffset;
    private final int maxResyncScanBytes;
    private final byte magicNumberFirstByte;
    private boolean discardingTooLongFrame;
    private long tooLongFrameLength;
    private long bytesToDiscard;
    private int frameLengthInt = -1;
    private boolean resyncing;
    private int resyncScannedBytes;

    /**
     * Creates a new instance.
     *
     * @param maxFrameLength
     *        the maximum length of the frame.  If the length of the frame is
     *        greater than this value, {@link TooLongFrameException} will be
     *        thrown.
     * @param lengthFieldOffset
     *        the offset of the length field
     * @param lengthFieldLength
     *        the length of the length field
     */
    public LengthFieldBasedFrameDecoder(
            int maxFrameLength,
            int lengthFieldOffset, int lengthFieldLength) {
        this(maxFrameLength, lengthFieldOffset, lengthFieldLength, 0, 0);
    }

    /**
     * Creates a new instance.
     *
     * @param maxFrameLength
     *        the maximum length of the frame.  If the length of the frame is
     *        greater than this value, {@link TooLongFrameException} will be
     *        thrown.
     * @param lengthFieldOffset
     *        the offset of the length field
     * @param lengthFieldLength
     *        the length of the length field
     * @param lengthAdjustment
     *        the compensation value to add to the value of the length field
     * @param initialBytesToStrip
     *        the number of first bytes to strip out from the decoded frame
     */
    public LengthFieldBasedFrameDecoder(
            int maxFrameLength,
            int lengthFieldOffset, int lengthFieldLength,
            int lengthAdjustment, int initialBytesToStrip) {
        this(
                maxFrameLength,
                lengthFieldOffset, lengthFieldLength, lengthAdjustment,
                initialBytesToStrip, true);
    }

    /**
     * Creates a new instance.
     *
     * @param maxFrameLength
     *        the maximum length of the frame.  If the length of the frame is
     *        greater than this value, {@link TooLongFrameException} will be
     *        thrown.
     * @param lengthFieldOffset
     *        the offset of the length field
     * @param lengthFieldLength
     *        the length of the length field
     * @param lengthAdjustment
     *        the compensation value to add to the value of the length field
     * @param initialBytesToStrip
     *        the number of first bytes to strip out from the decoded frame
     * @param failFast
     *        If <tt>true</tt>, a {@link TooLongFrameException} is thrown as
     *        soon as the decoder notices the length of the frame will exceed
     *        <tt>maxFrameLength</tt> regardless of whether the entire frame
     *        has been read.  If <tt>false</tt>, a {@link TooLongFrameException}
     *        is thrown after the entire frame that exceeds <tt>maxFrameLength</tt>
     *        has been read.
     */
    public LengthFieldBasedFrameDecoder(
            int maxFrameLength, int lengthFieldOffset, int lengthFieldLength,
            int lengthAdjustment, int initialBytesToStrip, boolean failFast) {
        this(
                ByteOrder.BIG_ENDIAN, maxFrameLength, lengthFieldOffset, lengthFieldLength,
                lengthAdjustment, initialBytesToStrip, failFast);
    }

    /**
     * Creates a new instance.
     *
     * @param byteOrder
     *        the {@link ByteOrder} of the length field
     * @param maxFrameLength
     *        the maximum length of the frame.  If the length of the frame is
     *        greater than this value, {@link TooLongFrameException} will be
     *        thrown.
     * @param lengthFieldOffset
     *        the offset of the length field
     * @param lengthFieldLength
     *        the length of the length field
     * @param lengthAdjustment
     *        the compensation value to add to the value of the length field
     * @param initialBytesToStrip
     *        the number of first bytes to strip out from the decoded frame
     * @param failFast
     *        If <tt>true</tt>, a {@link TooLongFrameException} is thrown as
     *        soon as the decoder notices the length of the frame will exceed
     *        <tt>maxFrameLength</tt> regardless of whether the entire frame
     *        has been read.  If <tt>false</tt>, a {@link TooLongFrameException}
     *        is thrown after the entire frame that exceeds <tt>maxFrameLength</tt>
     *        has been read.
     */
    public LengthFieldBasedFrameDecoder(
            ByteOrder byteOrder, int maxFrameLength, int lengthFieldOffset, int lengthFieldLength,
            int lengthAdjustment, int initialBytesToStrip, boolean failFast) {
        this(byteOrder, maxFrameLength, lengthFieldOffset, lengthFieldLength,
                lengthAdjustment, initialBytesToStrip, failFast, 0, 0, 0, 0);
    }

    /**
     * Creates a new instance that validates a magic number in the frame header before
     * decoding a frame.  A magic number mismatch results in a {@link CorruptedFrameException};
     * use {@link #LengthFieldBasedFrameDecoder(ByteOrder, int, int, int, int, int, boolean,
     * long, int, int, int)} to enable resynchronization instead.
     *
     * @param byteOrder
     *        the {@link ByteOrder} of the length field and the magic number
     * @param maxFrameLength
     *        the maximum length of the frame.  If the length of the frame is
     *        greater than this value, {@link TooLongFrameException} will be
     *        thrown.
     * @param lengthFieldOffset
     *        the offset of the length field
     * @param lengthFieldLength
     *        the length of the length field
     * @param lengthAdjustment
     *        the compensation value to add to the value of the length field
     * @param initialBytesToStrip
     *        the number of first bytes to strip out from the decoded frame
     * @param failFast
     *        If <tt>true</tt>, a {@link TooLongFrameException} is thrown as
     *        soon as the decoder notices the length of the frame will exceed
     *        <tt>maxFrameLength</tt> regardless of whether the entire frame
     *        has been read.  If <tt>false</tt>, a {@link TooLongFrameException}
     *        is thrown after the entire frame that exceeds <tt>maxFrameLength</tt>
     *        has been read.
     * @param magicNumber
     *        the magic number that must be present in the frame header, as an
     *        unsigned value that fits into {@code magicNumberLength} bytes
     * @param magicNumberOffset
     *        the offset of the magic number in the frame
     * @param magicNumberLength
     *        the length of the magic number: 1, 2, 3, 4, or 8
     */
    public LengthFieldBasedFrameDecoder(
            ByteOrder byteOrder, int maxFrameLength, int lengthFieldOffset, int lengthFieldLength,
            int lengthAdjustment, int initialBytesToStrip, boolean failFast,
            long magicNumber, int magicNumberOffset, int magicNumberLength) {
        this(byteOrder, maxFrameLength, lengthFieldOffset, lengthFieldLength,
                lengthAdjustment, initialBytesToStrip, failFast,
                magicNumber, magicNumberOffset, magicNumberLength, 0);
    }

    /**
     * Creates a new instance that validates a magic number in the frame header before
     * decoding a frame and resynchronizes on invalid data.
     * <p>
     * When the magic number does not match, or when {@code failFast} is <tt>false</tt>
     * and the length field at a magic-validated position is not plausible, the decoder
     * scans forward for the next magic number whose length field is self-consistent and
     * restarts frame detection there, silently discarding the skipped bytes.  A
     * {@link CorruptedFrameException} is thrown when no plausible magic number is found
     * within {@code maxResyncScanBytes} bytes.  See the class javadoc for the exact
     * rules.
     *
     * @param byteOrder
     *        the {@link ByteOrder} of the length field and the magic number
     * @param maxFrameLength
     *        the maximum length of the frame.  If the length of the frame is
     *        greater than this value, {@link TooLongFrameException} will be
     *        thrown.
     * @param lengthFieldOffset
     *        the offset of the length field
     * @param lengthFieldLength
     *        the length of the length field
     * @param lengthAdjustment
     *        the compensation value to add to the value of the length field
     * @param initialBytesToStrip
     *        the number of first bytes to strip out from the decoded frame
     * @param failFast
     *        If <tt>true</tt>, an untrustworthy length field at a magic-validated
     *        position results in an immediate {@link TooLongFrameException} or
     *        {@link CorruptedFrameException} instead of resynchronization.
     *        If <tt>false</tt>, such a position is treated as a false-positive
     *        magic number match and the decoder resynchronizes.
     * @param magicNumber
     *        the magic number that must be present in the frame header, as an
     *        unsigned value that fits into {@code magicNumberLength} bytes
     * @param magicNumberOffset
     *        the offset of the magic number in the frame
     * @param magicNumberLength
     *        the length of the magic number: 1, 2, 3, 4, or 8
     * @param maxResyncScanBytes
     *        the maximum number of bytes to scan while resynchronizing before
     *        giving up with a {@link CorruptedFrameException}.  Must be
     *        {@code > 0} to enable resynchronization.
     */
    public LengthFieldBasedFrameDecoder(
            ByteOrder byteOrder, int maxFrameLength, int lengthFieldOffset, int lengthFieldLength,
            int lengthAdjustment, int initialBytesToStrip, boolean failFast,
            long magicNumber, int magicNumberOffset, int magicNumberLength, int maxResyncScanBytes) {

        this.byteOrder = checkNotNull(byteOrder, "byteOrder");

        checkPositive(maxFrameLength, "maxFrameLength");

        checkPositiveOrZero(lengthFieldOffset, "lengthFieldOffset");

        checkPositiveOrZero(initialBytesToStrip, "initialBytesToStrip");

        if (lengthFieldOffset > maxFrameLength - lengthFieldLength) {
            throw new IllegalArgumentException(
                    "maxFrameLength (" + maxFrameLength + ") " +
                    "must be equal to or greater than " +
                    "lengthFieldOffset (" + lengthFieldOffset + ") + " +
                    "lengthFieldLength (" + lengthFieldLength + ").");
        }

        checkPositiveOrZero(magicNumberOffset, "magicNumberOffset");

        checkPositiveOrZero(maxResyncScanBytes, "maxResyncScanBytes");

        if (magicNumberLength != 0 && magicNumberLength != 1 && magicNumberLength != 2 &&
                magicNumberLength != 3 && magicNumberLength != 4 && magicNumberLength != 8) {
            throw new IllegalArgumentException(
                    "magicNumberLength must be 0 (disabled), 1, 2, 3, 4, or 8: " + magicNumberLength);
        }

        if (magicNumberLength > 0) {
            if (magicNumberOffset > maxFrameLength - magicNumberLength) {
                throw new IllegalArgumentException(
                        "maxFrameLength (" + maxFrameLength + ") " +
                        "must be equal to or greater than " +
                        "magicNumberOffset (" + magicNumberOffset + ") + " +
                        "magicNumberLength (" + magicNumberLength + ").");
            }
            if (magicNumberLength < 8 && (magicNumber < 0 || magicNumber >= 1L << (magicNumberLength * 8))) {
                throw new IllegalArgumentException(
                        "magicNumber (" + magicNumber + ") does not fit into " +
                        magicNumberLength + " bytes.");
            }
        }

        this.maxFrameLength = maxFrameLength;
        this.lengthFieldOffset = lengthFieldOffset;
        this.lengthFieldLength = lengthFieldLength;
        this.lengthAdjustment = lengthAdjustment;
        this.lengthFieldEndOffset = lengthFieldOffset + lengthFieldLength;
        this.initialBytesToStrip = initialBytesToStrip;
        this.failFast = failFast;
        this.magicNumber = magicNumber;
        this.magicNumberOffset = magicNumberOffset;
        this.magicNumberLength = magicNumberLength;
        this.magicNumberEndOffset = magicNumberOffset + magicNumberLength;
        this.maxResyncScanBytes = maxResyncScanBytes;
        if (magicNumberLength > 0) {
            // The first byte of the magic number in wire order, used to pre-filter scan positions.
            int shift = byteOrder == ByteOrder.BIG_ENDIAN ? (magicNumberLength - 1) * 8 : 0;
            this.magicNumberFirstByte = (byte) (magicNumber >>> shift);
        } else {
            this.magicNumberFirstByte = 0;
        }
    }

    @Override
    protected final void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        Object decoded = decode(ctx, in);
        if (decoded != null) {
            out.add(decoded);
        }
    }

    private void discardingTooLongFrame(ByteBuf in) {
        long bytesToDiscard = this.bytesToDiscard;
        int localBytesToDiscard = (int) Math.min(bytesToDiscard, in.readableBytes());
        in.skipBytes(localBytesToDiscard);
        bytesToDiscard -= localBytesToDiscard;
        this.bytesToDiscard = bytesToDiscard;

        failIfNecessary(false);
    }

    private static void failOnNegativeLengthField(ByteBuf in, long frameLength, int lengthFieldEndOffset) {
        in.skipBytes(lengthFieldEndOffset);
        throw new CorruptedFrameException(
           "negative pre-adjustment length field: " + frameLength);
    }

    private static void failOnFrameLengthLessThanLengthFieldEndOffset(ByteBuf in,
                                                                      long frameLength,
                                                                      int lengthFieldEndOffset) {
        in.skipBytes(lengthFieldEndOffset);
        throw new CorruptedFrameException(
           "Adjusted frame length (" + frameLength + ") is less " +
              "than lengthFieldEndOffset: " + lengthFieldEndOffset);
    }

    private void exceededFrameLength(ByteBuf in, long frameLength) {
        long discard = frameLength - in.readableBytes();
        tooLongFrameLength = frameLength;

        if (discard < 0) {
            // buffer contains more bytes then the frameLength so we can discard all now
            in.skipBytes((int) frameLength);
        } else {
            // Enter the discard mode and discard everything received so far.
            discardingTooLongFrame = true;
            bytesToDiscard = discard;
            in.skipBytes(in.readableBytes());
        }
        failIfNecessary(true);
    }

    private static void failOnFrameLengthLessThanInitialBytesToStrip(ByteBuf in,
                                                                     long frameLength,
                                                                     int initialBytesToStrip) {
        in.skipBytes((int) frameLength);
        throw new CorruptedFrameException(
           "Adjusted frame length (" + frameLength + ") is less " +
              "than initialBytesToStrip: " + initialBytesToStrip);
    }

    /**
     * Create a frame out of the {@link ByteBuf} and return it.
     *
     * @param   ctx             the {@link ChannelHandlerContext} which this {@link ByteToMessageDecoder} belongs to
     * @param   in              the {@link ByteBuf} from which to read data
     * @return  frame           the {@link ByteBuf} which represent the frame or {@code null} if no frame could
     *                          be created.
     */
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        if (magicNumberLength > 0) {
            return decodeWithMagicNumberValidation(ctx, in);
        }
        long frameLength = 0;
        if (frameLengthInt == -1) { // new frame

            if (discardingTooLongFrame) {
                discardingTooLongFrame(in);
            }

            if (in.readableBytes() < lengthFieldEndOffset) {
                return null;
            }

            int actualLengthFieldOffset = in.readerIndex() + lengthFieldOffset;
            frameLength = getUnadjustedFrameLength(in, actualLengthFieldOffset, lengthFieldLength, byteOrder);

            if (frameLength < 0) {
                failOnNegativeLengthField(in, frameLength, lengthFieldEndOffset);
            }

            frameLength += lengthAdjustment + lengthFieldEndOffset;

            if (frameLength < lengthFieldEndOffset) {
                failOnFrameLengthLessThanLengthFieldEndOffset(in, frameLength, lengthFieldEndOffset);
            }

            if (frameLength > maxFrameLength) {
                exceededFrameLength(in, frameLength);
                return null;
            }
            // never overflows because it's less than maxFrameLength
            frameLengthInt = (int) frameLength;
        }
        return readFrame(ctx, in);
    }

    private Object readFrame(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        if (in.readableBytes() < frameLengthInt) { // frameLengthInt exist , just check buf
            return null;
        }
        if (initialBytesToStrip > frameLengthInt) {
            // Reset the state before failing so the next frame is decoded again. Use frameLengthInt, as frameLength
            // is not available if the length field was decoded by a previous call.
            int frameLengthToSkip = frameLengthInt;
            frameLengthInt = -1;
            failOnFrameLengthLessThanInitialBytesToStrip(in, frameLengthToSkip, initialBytesToStrip);
        }
        in.skipBytes(initialBytesToStrip);

        // extract frame
        int readerIndex = in.readerIndex();
        int actualFrameLength = frameLengthInt - initialBytesToStrip;
        ByteBuf frame = extractFrame(ctx, in, readerIndex, actualFrameLength);
        in.readerIndex(readerIndex + actualFrameLength);
        frameLengthInt = -1; // start processing the next frame
        return frame;
    }

    /**
     * Decodes a frame when magic number validation is enabled.  The magic number is
     * checked before the length field is consulted; on a mismatch the decoder either
     * fails (resynchronization disabled) or scans forward for the next plausible magic
     * number (resynchronization enabled).  See the class javadoc for the exact rules,
     * including how {@code failFast} is honored.
     */
    private Object decodeWithMagicNumberValidation(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        if (frameLengthInt == -1) { // new frame

            if (discardingTooLongFrame) {
                discardingTooLongFrame(in);
            }

            int headerEndOffset = Math.max(lengthFieldEndOffset, magicNumberEndOffset);
            while (frameLengthInt == -1) {
                if (in.readableBytes() < headerEndOffset) {
                    // Not even a complete header (e.g. half a frame was received):
                    // wait for more data, never scan or fail on a partial header.
                    return null;
                }

                if (!resyncing && !matchesMagicNumber(in, in.readerIndex())) {
                    if (!isResyncEnabled()) {
                        throw new CorruptedFrameException(
                                "magic number mismatch at offset " + magicNumberOffset +
                                ": expected " + magicNumber);
                    }
                    startResync();
                }

                if (resyncing) {
                    if (!tryResync(in)) {
                        return null;
                    }
                    // The reader index now points at a validated candidate, re-check the header.
                    continue;
                }

                long frameLength = getUnadjustedFrameLength(in, in.readerIndex() + lengthFieldOffset,
                        lengthFieldLength, byteOrder);

                if (frameLength < 0) {
                    if (isResyncEnabled() && !failFast) {
                        // False-positive magic number match: resynchronize instead of failing.
                        startResync();
                        continue;
                    }
                    failOnNegativeLengthField(in, frameLength, lengthFieldEndOffset);
                }

                frameLength += lengthAdjustment + lengthFieldEndOffset;

                if (frameLength < lengthFieldEndOffset) {
                    if (isResyncEnabled() && !failFast) {
                        startResync();
                        continue;
                    }
                    failOnFrameLengthLessThanLengthFieldEndOffset(in, frameLength, lengthFieldEndOffset);
                }

                if (frameLength > maxFrameLength) {
                    if (isResyncEnabled() && !failFast) {
                        startResync();
                        continue;
                    }
                    exceededFrameLength(in, frameLength);
                    return null;
                }
                // never overflows because it's less than maxFrameLength
                frameLengthInt = (int) frameLength;
            }
        }
        return readFrame(ctx, in);
    }

    private boolean isResyncEnabled() {
        return maxResyncScanBytes > 0;
    }

    private void startResync() {
        resyncing = true;
        resyncScannedBytes = 0;
    }

    /**
     * Scans forward for the next magic number whose length field is self-consistent.
     * The scanned bytes are consumed from the buffer, so the scan position is implicitly
     * maintained across {@code decode} invocations and stays correct when the cumulative
     * buffer is compacted (see {@link ByteToMessageDecoder#discardSomeReadBytes()}).
     *
     * @return {@code true} if the reader index now points at a validated candidate,
     *         {@code false} if more data is needed
     * @throws CorruptedFrameException if no plausible magic number was found within
     *         {@code maxResyncScanBytes} bytes
     */
    private boolean tryResync(ByteBuf in) {
        for (;;) {
            int candidate = indexOfMagicNumber(in);
            if (candidate < 0) {
                // No magic number in the readable bytes. Consume everything but the tail
                // that may hold a partially received magic number.
                int consumed = in.readableBytes() - (magicNumberLength - 1);
                if (consumed > 0) {
                    in.skipBytes(consumed);
                    resyncScannedBytes += consumed;
                    checkResyncScanWindow();
                }
                return false;
            }
            int skipped = candidate - in.readerIndex();
            if (skipped > 0) {
                in.skipBytes(skipped);
                resyncScannedBytes += skipped;
                checkResyncScanWindow();
            }
            if (in.readableBytes() < lengthFieldEndOffset) {
                // Candidate found but the length field was not fully received yet: wait for more data.
                return false;
            }
            if (isLengthFieldPlausible(in)) {
                resyncing = false;
                resyncScannedBytes = 0;
                return true;
            }
            // False positive (e.g. magic number bytes inside a length field or a payload):
            // skip one byte and keep scanning.
            in.skipBytes(1);
            resyncScannedBytes++;
            checkResyncScanWindow();
        }
    }

    private void checkResyncScanWindow() {
        if (resyncScannedBytes > maxResyncScanBytes) {
            resyncing = false;
            resyncScannedBytes = 0;
            throw new CorruptedFrameException(
                    "unable to resynchronize: no plausible magic number found within " +
                    maxResyncScanBytes + " bytes");
        }
    }

    /**
     * Returns {@code true} if the length field at the current reader index decodes to a
     * frame length that is consistent with the decoder configuration.  Used to suppress
     * false-positive magic number matches while resynchronizing.
     */
    private boolean isLengthFieldPlausible(ByteBuf in) {
        long frameLength = getUnadjustedFrameLength(in, in.readerIndex() + lengthFieldOffset,
                lengthFieldLength, byteOrder);
        if (frameLength < 0) {
            return false;
        }
        frameLength += lengthAdjustment + lengthFieldEndOffset;
        return frameLength >= lengthFieldEndOffset &&
                frameLength >= initialBytesToStrip &&
                frameLength <= maxFrameLength;
    }

    private boolean matchesMagicNumber(ByteBuf in, int offset) {
        ByteBuf buf = in.order(byteOrder);
        long actual;
        switch (magicNumberLength) {
        case 1:
            actual = buf.getUnsignedByte(offset);
            break;
        case 2:
            actual = buf.getUnsignedShort(offset);
            break;
        case 3:
            actual = buf.getUnsignedMedium(offset);
            break;
        case 4:
            actual = buf.getUnsignedInt(offset);
            break;
        case 8:
            actual = buf.getLong(offset);
            break;
        default:
            throw new DecoderException(
                    "unsupported magicNumberLength: " + magicNumberLength + " (expected: 1, 2, 3, 4, or 8)");
        }
        return actual == magicNumber;
    }

    /**
     * Returns the index of the next magic number occurrence at or after the reader index,
     * or {@code -1} if there is none in the readable bytes.
     */
    private int indexOfMagicNumber(ByteBuf in) {
        int index = in.readerIndex();
        int end = in.writerIndex() - magicNumberLength + 1;
        while (index < end) {
            index = in.indexOf(index, end, magicNumberFirstByte);
            if (index < 0) {
                return -1;
            }
            if (matchesMagicNumber(in, index)) {
                return index;
            }
            index++;
        }
        return -1;
    }

    /**
     * Decodes the specified region of the buffer into an unadjusted frame length.  The default implementation is
     * capable of decoding the specified region into an unsigned 8/16/24/32/64 bit integer.  Override this method to
     * decode the length field encoded differently.  Note that this method must not modify the state of the specified
     * buffer (e.g. {@code readerIndex}, {@code writerIndex}, and the content of the buffer.)
     *
     * @throws DecoderException if failed to decode the specified region
     */
    protected long getUnadjustedFrameLength(ByteBuf buf, int offset, int length, ByteOrder order) {
        buf = buf.order(order);
        long frameLength;
        switch (length) {
        case 1:
            frameLength = buf.getUnsignedByte(offset);
            break;
        case 2:
            frameLength = buf.getUnsignedShort(offset);
            break;
        case 3:
            frameLength = buf.getUnsignedMedium(offset);
            break;
        case 4:
            frameLength = buf.getUnsignedInt(offset);
            break;
        case 8:
            frameLength = buf.getLong(offset);
            break;
        default:
            throw new DecoderException(
                    "unsupported lengthFieldLength: " + lengthFieldLength + " (expected: 1, 2, 3, 4, or 8)");
        }
        return frameLength;
    }

    private void failIfNecessary(boolean firstDetectionOfTooLongFrame) {
        if (bytesToDiscard == 0) {
            // Reset to the initial state and tell the handlers that
            // the frame was too large.
            long tooLongFrameLength = this.tooLongFrameLength;
            this.tooLongFrameLength = 0;
            discardingTooLongFrame = false;
            if (!failFast || firstDetectionOfTooLongFrame) {
                fail(tooLongFrameLength);
            }
        } else {
            // Keep discarding and notify handlers if necessary.
            if (failFast && firstDetectionOfTooLongFrame) {
                fail(tooLongFrameLength);
            }
        }
    }

    /**
     * Extract the sub-region of the specified buffer.
     */
    protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
        return buffer.retainedSlice(index, length);
    }

    private void fail(long frameLength) {
        if (frameLength > 0) {
            throw new TooLongFrameException(
                            "Adjusted frame length exceeds " + maxFrameLength +
                            ": " + frameLength + " - discarded");
        } else {
            throw new TooLongFrameException(
                            "Adjusted frame length exceeds " + maxFrameLength +
                            " - discarding");
        }
    }
}
