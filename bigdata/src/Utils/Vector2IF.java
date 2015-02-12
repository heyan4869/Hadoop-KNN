package Utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Vector2IF
 * 
 * This is wrote by Yan He inspired by the original work that done by Song Liu (sl9885)
 */
public class Vector2IF extends Vector2<Integer, Float, Integer> implements Writable {
	public Vector2IF() {
	}

	public Vector2IF(Integer v1, Float v2, Integer v3) {
		this.v1 = v1;
		this.v2 = v2;
		this.v3 = v3;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		v1 = in.readInt();
		v2 = in.readFloat();
		v3 = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(v1);
		out.writeFloat(v2);
		out.writeInt(v3);
	}
}