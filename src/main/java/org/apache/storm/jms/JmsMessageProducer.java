package org.apache.storm.jms;

import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
/**
 * JmsMessageProducer implementations are responsible for translating
 * a <code>org.apache.storm.tuple.Values</code> instance into a
 * <code>javax.jms.Message</code> object.
 * <p/>
 * 
 * 
 * @author P. Taylor Goetz
 *
 */
public interface JmsMessageProducer extends Serializable{
	
	/**
	 * Translate a <code>org.apache.storm.tuple.Tuple</code> object
	 * to a <code>javax.jms.Message</code object.
	 * 
	 * @param session
	 * @param input
	 * @return
	 * @throws JMSException
	 */
	public Message toMessage(Session session, Tuple input) throws JMSException;
}
