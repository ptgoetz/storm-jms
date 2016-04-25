package org.apache.storm.jms.trident;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.trident.tuple.TridentTuple;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import java.io.Serializable;

public interface TridentJmsMessageProducer extends Serializable{

	/**
	 * Translate a <code>org.apache.storm.tuple.TridentTuple</code> object
	 * to a <code>javax.jms.Message</code object.
	 *
	 * @param session
	 * @param input
	 * @return
	 * @throws javax.jms.JMSException
	 */
	public Message toMessage(Session session, TridentTuple input) throws JMSException;
}
