package cz.company.guardian.validation;

import java.util.Random;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchemaIdSetTest {

    @Test
    void containsExactlyTheGivenIds() {
        SchemaIdSet set = SchemaIdSet.of(1, 2, 3);

        assertThat(set.contains(1)).isTrue();
        assertThat(set.contains(2)).isTrue();
        assertThat(set.contains(3)).isTrue();
        assertThat(set.contains(0)).isFalse();
        assertThat(set.contains(4)).isFalse();
        assertThat(set.contains(-1)).isFalse();
        assertThat(set.size()).isEqualTo(3);
    }

    @Test
    void ignoresDuplicates() {
        SchemaIdSet set = SchemaIdSet.of(7, 7, 7);

        assertThat(set.size()).isEqualTo(1);
        assertThat(set.toSortedArray()).containsExactly(7);
    }

    @Test
    void supportsBoundaryValues() {
        SchemaIdSet set = SchemaIdSet.of(0, Integer.MAX_VALUE);

        assertThat(set.contains(0)).isTrue();
        assertThat(set.contains(Integer.MAX_VALUE)).isTrue();
        assertThat(set.contains(Integer.MIN_VALUE)).isFalse();
    }

    @Test
    void rejectsNegativeIds() {
        assertThatThrownBy(() -> SchemaIdSet.of(1, -1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void agreesWithHashSetForManyRandomIds() {
        Random random = new Random(1234);
        java.util.Set<Integer> reference = new java.util.HashSet<>();
        int[] ids = new int[5000];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = random.nextInt(Integer.MAX_VALUE);
            reference.add(ids[i]);
        }
        SchemaIdSet set = SchemaIdSet.of(ids);

        assertThat(set.size()).isEqualTo(reference.size());
        for (int id : ids) {
            assertThat(set.contains(id)).isTrue();
        }
        for (int i = 0; i < 20_000; i++) {
            int probe = random.nextInt(Integer.MAX_VALUE);
            assertThat(set.contains(probe)).isEqualTo(reference.contains(probe));
        }
    }

    @Test
    void emptySetContainsNothingAndTerminates() {
        SchemaIdSet set = SchemaIdSet.of();

        assertThat(set.contains(0)).isFalse();
        assertThat(set.size()).isZero();
    }

    @Test
    void sequentialIdsThatCollideStillResolve() {
        int[] ids = new int[1000];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = i * 1024;
        }
        SchemaIdSet set = SchemaIdSet.of(ids);

        for (int id : ids) {
            assertThat(set.contains(id)).isTrue();
            assertThat(set.contains(id + 1)).isFalse();
        }
    }
}
