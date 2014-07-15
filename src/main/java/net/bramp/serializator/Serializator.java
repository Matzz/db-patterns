package net.bramp.serializator;

public interface Serializator<E> {
	public byte[] serialize(E obj);
	public E deserialize(byte[] bytes);
}
