import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

public class UserActioninInfo implements Writable {

    private String userLogin;
    private long time;


    @Override
    public void write(DataOutput dataOutput) throws IOException {
             dataOutput.writeUTF(this.userLogin);
             dataOutput.writeLong(this.time);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
            this.userLogin = dataInput.readUTF();
            this.time = dataInput.readLong();
    }
    @Override
    public String toString() {
        return "UserActionInfo{"+"userLogin='"+userLogin+"\'"+", time"+new Date(time)+"}";

    }
}
