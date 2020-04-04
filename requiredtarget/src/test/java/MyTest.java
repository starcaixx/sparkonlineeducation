import java.time.Instant;

/**
 * @author star
 * @create 2019-09-04 19:26
 */
public class MyTest {
    public static void main(String[] args) {
        System.out.println(Instant.now().getEpochSecond());
        StringBuilder sb = new StringBuilder("alfeiajfleafea,\n,\n,");
        System.out.println(sb.deleteCharAt(sb.lastIndexOf(",")));
    }
}
