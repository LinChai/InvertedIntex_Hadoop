import java.lang.String;
import java.util.*;
import java.io.*;

import org.apache.hadoop.io.Writable;
class Posting {
	Posting(int d, int p) {
		this.docid = d;
		this.pos = p;
	}
	int docid;
	int pos;
}
public class CompositeWritable implements Writable {
    int val1 = 0;
    int val2 = 0;
    int val3 = 0;
    List<Posting> ls = new ArrayList<>();

    public CompositeWritable() {}

    public CompositeWritable(int val1, int val2, int val3) {
        this.val1 = val1;
        this.val2 = val2;
        this.val3 = val3;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        val1 = in.readInt();
        val2 = in.readInt();
        val3 = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(val1);
        out.writeInt(val2);
        out.writeInt(val3);
    }

    public void merge(CompositeWritable other) {
        this.val1 += other.val1;
	this.ls.add(new Posting(other.val2, other.val3));
    }

    @Override
    public String toString() {
	StringBuilder sb = new StringBuilder();
	sb.append(this.val1 );
	sb.append("\t");
	for(int i = 0; i < ls.size(); i++) {
		sb.append("(");
		sb.append(ls.get(i).docid);
		sb.append(",");
		sb.append(ls.get(i).pos);
		sb.append(") ");
	}	
        return sb.toString(); 
    }
}
