package net.bramp.serializator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class DefaultSerializator<E> implements Serializator<E> {

	@Override
	public byte[] serialize(E obj) {
		byte[] array = null;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			try {
				ObjectOutputStream objectOut = new ObjectOutputStream(out);
				try {
					objectOut.writeObject(obj);
					array = out.toByteArray();
				} catch (IOException e) {
					objectOut.close();
				}
			} finally {
				out.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return array;
	}

	@Override
	public E deserialize(byte[] bytes) {
		E obj = null;
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		try {
			try {
				ObjectInputStream objectIn = new ObjectInputStream(in);
				try {
					obj = (E) objectIn.readObject();
				} catch (IOException e) {
					objectIn.close();
				} catch (ClassNotFoundException e) {
					throw new RuntimeException(e);
				}
			} finally {
				in.close();
			}

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return obj;
	}

}
