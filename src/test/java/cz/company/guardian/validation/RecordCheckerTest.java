package cz.company.guardian.validation;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

class RecordCheckerTest {

    private static final SchemaIdSet ALLOWED = SchemaIdSet.of(0, 1, 2, 3, Integer.MAX_VALUE);

    private static ByteBuffer frame(int magic, int schemaId, int payloadBytes) {
        ByteBuffer buffer = ByteBuffer.allocate(5 + payloadBytes);
        buffer.put((byte) magic).putInt(schemaId);
        for (int i = 0; i < payloadBytes; i++) {
            buffer.put((byte) 0x7F);
        }
        buffer.flip();
        return buffer;
    }

    @Test
    void allowsWhitelistedSchemaId() {
        assertThat(RecordChecker.check(frame(0, 2, 10), ALLOWED)).isNull();
    }

    @Test
    void allowsHeaderOnlyValueWithEmptyPayload() {
        assertThat(RecordChecker.check(frame(0, 1, 0), ALLOWED)).isNull();
    }

    @Test
    void rejectsSchemaIdNotInWhitelist() {
        assertThat(RecordChecker.check(frame(0, 4, 10), ALLOWED)).isEqualTo(RejectionReason.SCHEMA_NOT_ALLOWED);
    }

    @Test
    void rejectsNullValue() {
        assertThat(RecordChecker.check(null, ALLOWED)).isEqualTo(RejectionReason.NULL_VALUE);
    }

    @ParameterizedTest
    @MethodSource("shortLengths")
    void rejectsValueShorterThanFiveBytes(int length) {
        ByteBuffer value = ByteBuffer.allocate(length);
        assertThat(RecordChecker.check(value, ALLOWED)).isEqualTo(RejectionReason.PAYLOAD_TOO_SHORT);
    }

    static Stream<Integer> shortLengths() {
        return Stream.of(0, 1, 2, 3, 4);
    }

    @ParameterizedTest
    @MethodSource("badMagicBytes")
    void rejectsWrongMagicByte(int magic) {
        assertThat(RecordChecker.check(frame(magic, 1, 3), ALLOWED)).isEqualTo(RejectionReason.INVALID_MAGIC_BYTE);
    }

    static Stream<Integer> badMagicBytes() {
        return Stream.of(1, 2, 0x7F, 0x80, 0xFF);
    }

    @Test
    void schemaIdZeroIsAValidId() {
        assertThat(RecordChecker.check(frame(0, 0, 1), ALLOWED)).isNull();
        assertThat(RecordChecker.check(frame(0, 0, 1), SchemaIdSet.of(1))).isEqualTo(RejectionReason.SCHEMA_NOT_ALLOWED);
    }

    @Test
    void handlesMaximumPositiveInt32SchemaId() {
        assertThat(RecordChecker.check(frame(0, Integer.MAX_VALUE, 1), ALLOWED)).isNull();
    }

    @Test
    void idWithHighBitSetReadsAsNegativeAndIsNeverAllowed() {
        assertThat(RecordChecker.check(frame(0, Integer.MIN_VALUE, 1), ALLOWED)).isEqualTo(RejectionReason.SCHEMA_NOT_ALLOWED);
        assertThat(RecordChecker.check(frame(0, -1, 1), ALLOWED)).isEqualTo(RejectionReason.SCHEMA_NOT_ALLOWED);
    }

    @Test
    void schemaIdIsBigEndian() {
        ByteBuffer value = ByteBuffer.wrap(new byte[]{ 0, 0x00, 0x00, 0x01, 0x00, 'x' });
        assertThat(RecordChecker.peekSchemaId(value)).isEqualTo(256L);
        assertThat(RecordChecker.check(value, SchemaIdSet.of(256))).isNull();
        assertThat(RecordChecker.check(value, SchemaIdSet.of(1))).isEqualTo(RejectionReason.SCHEMA_NOT_ALLOWED);
    }

    @Test
    void doesNotChangePositionLimitOrMark() {
        ByteBuffer value = frame(0, 3, 20);
        value.position(0).mark();
        int limit = value.limit();

        RecordChecker.check(value, ALLOWED);
        RecordChecker.peekSchemaId(value);

        assertThat(value.position()).isZero();
        assertThat(value.limit()).isEqualTo(limit);
        value.position(7);
        value.reset();
        assertThat(value.position()).isZero();
    }

    @Test
    void honoursNonZeroPositionOfTheBuffer() {
        ByteBuffer backing = ByteBuffer.allocate(32);
        backing.put(new byte[]{ 9, 9, 9 });
        backing.put(frame(0, 2, 4));
        backing.flip();
        backing.position(3);

        assertThat(RecordChecker.check(backing, ALLOWED)).isNull();
        assertThat(backing.position()).isEqualTo(3);
    }

    @Test
    void worksWithDirectBuffer() {
        ByteBuffer direct = ByteBuffer.allocateDirect(16);
        direct.put(frame(0, 1, 4)).flip();

        assertThat(RecordChecker.check(direct, ALLOWED)).isNull();
        assertThat(direct.position()).isZero();
    }

    @Test
    void worksWithReadOnlyBuffer() {
        ByteBuffer readOnly = frame(0, 5, 4).asReadOnlyBuffer();

        assertThat(RecordChecker.check(readOnly, ALLOWED)).isEqualTo(RejectionReason.SCHEMA_NOT_ALLOWED);
        assertThat(RecordChecker.check(frame(0, 3, 4).asReadOnlyBuffer(), ALLOWED)).isNull();
    }

    @Test
    void worksWithSlicedBuffer() {
        ByteBuffer parent = ByteBuffer.allocate(64);
        parent.put(new byte[]{ 1, 2, 3, 4, 5, 6, 7 });
        parent.put(frame(0, 2, 4));
        parent.position(7);
        ByteBuffer slice = parent.slice();
        slice.limit(9);

        assertThat(RecordChecker.check(slice, ALLOWED)).isNull();
        assertThat(slice.position()).isZero();
        assertThat(parent.position()).isEqualTo(7);
    }

    @Test
    void sliceShorterThanHeaderIsRejectedEvenIfParentHasMoreBytes() {
        ByteBuffer parent = frame(0, 2, 20);
        ByteBuffer slice = parent.duplicate().limit(4).slice();

        assertThat(RecordChecker.check(slice, ALLOWED)).isEqualTo(RejectionReason.PAYLOAD_TOO_SHORT);
    }

    @Test
    void honoursBufferByteOrderOtherThanBigEndian() {
        ByteBuffer value = frame(0, 3, 2).order(ByteOrder.LITTLE_ENDIAN);

        assertThat(RecordChecker.check(value, ALLOWED)).isNull();
        assertThat(RecordChecker.peekSchemaId(value)).isEqualTo(3L);
        assertThat(value.order()).isEqualTo(ByteOrder.LITTLE_ENDIAN);
    }

    @Test
    void peekReturnsSentinelWhenNoSchemaIdIsReadable() {
        assertThat(RecordChecker.peekSchemaId(null)).isEqualTo(RecordChecker.NO_SCHEMA_ID);
        assertThat(RecordChecker.peekSchemaId(ByteBuffer.allocate(3))).isEqualTo(RecordChecker.NO_SCHEMA_ID);
        assertThat(RecordChecker.peekSchemaId(frame(1, 7, 1))).isEqualTo(RecordChecker.NO_SCHEMA_ID);
    }
}
