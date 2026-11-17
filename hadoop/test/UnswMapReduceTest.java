import java.math.BigInteger;
import org.apache.hadoop.io.Text;

public class UnswMapReduceTest {
  static void require(boolean condition, String message) { if (!condition) throw new AssertionError(message); }
  static void invalid(String value) { try { UnswMapReduce.totals(value); throw new AssertionError("accepted: " + value); } catch (IllegalArgumentException expected) {} }
  public static void main(String[] args) {
    UnswMapReduce.Totals one = UnswMapReduce.totals("1,4200,1");
    require(UnswMapReduce.intermediate(one).equals("1,4200,1"), "round trip");
    java.util.List<Text> parts = java.util.Arrays.asList(new Text("1,4200,1"), new Text("2,7,0"));
    UnswMapReduce.Totals combined = UnswMapReduce.sum(parts);
    require(UnswMapReduce.intermediate(combined).equals("3,4207,1"), "combine");
    UnswMapReduce.Totals large = UnswMapReduce.sum(java.util.Arrays.asList(new Text("1,9223372036854775808,0"), new Text("1,1,1")));
    require(large.bytes.equals(new BigInteger("9223372036854775809")), "BigInteger bytes");
    invalid("1\t4200\t1"); invalid("1,not-a-number,0"); invalid("1,2");
    System.out.println("UNSW_MAPREDUCE_TEST_OK");
  }
}
