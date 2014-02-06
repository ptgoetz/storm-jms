package backtype.storm.contrib.jms;

import java.io.Serializable;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
/**
 * A <code>JmsProvider</code> object encapsulates the <code>ConnectionFactory</code>
 * and <code>Destination</code> JMS objects the <code>JmsSpout</code> needs to manage
 * a topic/queue connection over the course of it's lifecycle.
 * 
 * @author P. Taylor Goetz
 *
 */
public interface JmsProvider extends Serializable{

    /**
     * @return name, which identifies this specific provider instance in order to name dependent resources like
     * consumer threads.
     */
    String getName();

	/**
	 * Provides the JMS <code>ConnectionFactory</code>
	 * @return the connection factory
	 * @throws Exception
	 */
	public ConnectionFactory connectionFactory() throws Exception;

	/**
	 * Provides the <code>Destination</code> (topic or queue) from which the
	 * <code>JmsSpout</code> will receive messages.
	 * @return
	 * @throws Exception
	 */
	public Destination destination() throws Exception;
    
    /**
     * Provides an optional message selector for consuming messages.
     * Only messages with properties matching the message selector expression are delivered. 
     * A value of null or an empty string indicates that there is no message selector for the message consumer.
     * @return see description
     */
    public String messageSelector();
}
