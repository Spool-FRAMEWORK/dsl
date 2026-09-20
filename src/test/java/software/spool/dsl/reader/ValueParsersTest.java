package software.spool.dsl.reader;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ValueParsersTest {

    private enum Colour { RED, GREEN }

    @Test
    void string_acceptsPlainValues() {
        assertThat(ValueParsers.STRING.parse("poll")).contains("poll");
        assertThat(ValueParsers.STRING.parse(5)).contains("5");
        assertThat(ValueParsers.STRING.parse(true)).contains("true");
    }

    @Test
    void string_rejectsObjectsAndLists() {
        assertThat(ValueParsers.STRING.parse(Map.of())).isEmpty();
        assertThat(ValueParsers.STRING.parse(List.of())).isEmpty();
    }

    @Test
    void integer_acceptsNumbersAndNumericText() {
        assertThat(ValueParsers.INTEGER.parse(5000)).contains(5000);
        assertThat(ValueParsers.INTEGER.parse(" 42 ")).contains(42);
        assertThat(ValueParsers.INTEGER.parse("-7")).contains(-7);
    }

    @Test
    void integer_rejectsWhatIsNotAWholeNumberThatFits() {
        assertThat(ValueParsers.INTEGER.parse("abc")).isEmpty();
        assertThat(ValueParsers.INTEGER.parse(1.5)).isEmpty();
        assertThat(ValueParsers.INTEGER.parse(3_000_000_000L)).isEmpty();
        assertThat(ValueParsers.INTEGER.parse("99999999999999999999")).isEmpty();
    }

    @Test
    void long_acceptsValuesBeyondTheIntegerRange() {
        assertThat(ValueParsers.LONG.parse(3_000_000_000L)).contains(3_000_000_000L);
    }

    @Test
    void oneOf_acceptsTheExactNameOfAConstant() {
        assertThat(ValueParsers.oneOf(Colour.class).parse("GREEN")).contains(Colour.GREEN);
    }

    @Test
    void oneOf_rejectsAnythingElse() {
        assertThat(ValueParsers.oneOf(Colour.class).parse("green")).isEmpty();
        assertThat(ValueParsers.oneOf(Colour.class).parse("BLUE")).isEmpty();
        assertThat(ValueParsers.oneOf(Colour.class).parse(3)).isEmpty();
    }

    @Test
    void oneOf_listsTheValidConstantsInItsMessage() {
        assertThat(ValueParsers.oneOf(Colour.class).expected()).isEqualTo("one of RED, GREEN");
    }
}
