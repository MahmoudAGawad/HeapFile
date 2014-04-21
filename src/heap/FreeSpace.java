package heap;

public class FreeSpace implements Comparable<FreeSpace>{
	
	private int key, value;
	
	public FreeSpace(int k, int v){
		key=k;
		value=v;
	}
	
	@Override
	public int compareTo(FreeSpace o) {
		return o.key - key;
	}
	
	public int getKey() {
		return key;
	}
	
	public int getValue() {
		return value;
	}
}
