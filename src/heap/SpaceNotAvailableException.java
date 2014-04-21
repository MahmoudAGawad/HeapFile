package heap;

import chainexception.ChainException;

public class SpaceNotAvailableException extends ChainException{
	
	public SpaceNotAvailableException(Exception e,String name){
		super(e,name);
	}
	
}
