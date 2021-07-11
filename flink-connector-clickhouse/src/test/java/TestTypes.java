import java.sql.JDBCType;
import org.junit.Test;

public class TestTypes {

  @Test
  public void testType(){
    System.out.println(JDBCType.BINARY.getVendorTypeNumber());
    System.out.println(JDBCType.BINARY.getName());
  }
}
