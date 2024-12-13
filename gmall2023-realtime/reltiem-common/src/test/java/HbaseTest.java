import com.bw.gmall.realtime.common.util.HBaseutill;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.client.Connection;

public class HbaseTest {
    @SneakyThrows
    public static void main(String[] args) {
        Connection hBaseConnection = HBaseutill.getHBaseConnection();
        HBaseutill.createHBaseTable(hBaseConnection,"gmall_dev","test_t1","id,name");
    }
}
