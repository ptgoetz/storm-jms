package backtype.storm.contrib.jms.spout;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import backtype.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.contrib.jms.JmsProvider;
import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Values;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import javax.jms.MessageProducer;

/**
 * A Storm <code>Spout</code> implementation that pulls messages from a JMS topic or queue and outputs tuples based on
 * the messages it receives. It processes messages in batches and messages which fail twice to be delivered will be
 * expired (poisoned message protection).
 *
 * <p/>
 * <code>SynchronousJmsSpout</code> instances rely on <code>JmsProducer</code> implementations to obtain the JMS
 * <code>ConnectionFactory</code> and <code>Destination</code> objects necessary to connect to a JMS topic/queue.
 * <p/>
 * When a <code>SynchronousJmsSpout</code> receives a JMS message, it delegates to an internal
 * <code>JmsTupleProducer</code> instance to create a Storm tuple from the incoming message.
 * <p/>
 * Typically, developers will supply a custom <code>JmsTupleProducer</code> implementation appropriate for the expected
 * message content.
 * 
 * based on code from P. Taylor Goetz
 *
 * @author Winfried Umbrath
 *
 */
@SuppressWarnings("serial")
public class SynchronousJmsSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(SynchronousJmsSpout.class);

    private int batchSize;
    private static final int DEFAULT_BATCH_SIZE = 50;

    // JMS options
    private int jmsAcknowledgeMode = Session.AUTO_ACKNOWLEDGE;

    private boolean distributed = true;

    private JmsTupleProducer tupleProducer;

    private JmsProvider jmsProvider;

    private ConcurrentHashMap<String, Message> pendingMessages;

    private SpoutOutputCollector collector;

    private transient Connection connection;
    private transient Session session;
    private transient MessageConsumer consumer;

    private volatile boolean hasFailures = false;
    private final AtomicInteger msgCounter = new AtomicInteger(0);

    public SynchronousJmsSpout() {
        batchSize = DEFAULT_BATCH_SIZE;
    }
    
    /**
     * Constructs a <code>SynchronousJmsSpout</code>
     * @param _batchSize Defines the batch size to be used.
     */
    public SynchronousJmsSpout(int _batchSize) {
        batchSize = _batchSize;
    }

    /**
     * Sets the JMS Session acknowledgement mode for the JMS seesion associated with this spout.
     * <p/>
     * Possible values:
     * <ul>
     * <li>javax.jms.Session.AUTO_ACKNOWLEDGE</li>
     * <li>javax.jms.Session.CLIENT_ACKNOWLEDGE</li>
     * <li>javax.jms.Session.DUPS_OK_ACKNOWLEDGE</li>
     * </ul>
     *
     * @param _mode JMS Session Acknowledgement mode
     * @throws IllegalArgumentException if the mode is not recognized.
     */
    public void setJmsAcknowledgeMode(int _mode) {
        switch (_mode) {
            case Session.AUTO_ACKNOWLEDGE:
            case Session.CLIENT_ACKNOWLEDGE:
            case Session.DUPS_OK_ACKNOWLEDGE:
                break;
            default:
                throw new IllegalArgumentException("Unknown Acknowledge mode: " + _mode + " (See javax.jms.Session for valid values)");

        }
        jmsAcknowledgeMode = _mode;
    }

    /**
     * Returns the JMS Session acknowledgement mode for the JMS seesion associated with this spout.
     *
     * @return
     */
    public int getJmsAcknowledgeMode() {
        return jmsAcknowledgeMode;
    }

    /**
     * Set the <code>backtype.storm.contrib.jms.JmsProvider</code> implementation that this Spout will use to connect to
     * a JMS <code>javax.jms.Desination</code>
     *
     * @param _provider
     */
    public void setJmsProvider(JmsProvider _provider) {
        jmsProvider = _provider;
    }

    /**
     * Set the <code>backtype.storm.contrib.jms.JmsTupleProducer</code> implementation that will convert
     * <code>javax.jms.Message</code> object to <code>backtype.storm.tuple.Values</code> objects to be emitted.
     *
     * @param _producer
     */
    public void setJmsTupleProducer(JmsTupleProducer _producer) {
        tupleProducer = _producer;
    }

    /**
     * <code>ISpout</code> implementation.
     * <p/>
     * Connects the JMS spout to the configured JMS destination topic/queue.
     *
     */
    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector _collector) {
        if (jmsProvider == null) {
            throw new IllegalStateException("JMS provider has not been set.");
        }
        if (tupleProducer == null) {
            throw new IllegalStateException("JMS Tuple Producer has not been set.");
        }

        pendingMessages = new ConcurrentHashMap<String, Message>();
        collector = _collector;
        try {
            ConnectionFactory cf = jmsProvider.connectionFactory();
            Destination dest = jmsProvider.destination();
            connection = cf.createConnection();
            session = connection.createSession(false,
                    jmsAcknowledgeMode);
            consumer = session.createConsumer(dest, jmsProvider.messageSelector());
            connection.start();

        } catch (Exception e) {
            LOG.warn("Error creating JMS connection.", e);
        }

    }

    @Override
    public void close() {
        try {
            LOG.debug("Closing JMS connection.");
            session.close();
            connection.close();
        } catch (JMSException e) {
            LOG.warn("Error closing JMS connection.", e);
        }

    }

    @Override
    public void nextTuple() {
        Message msg = null;
        try {
            if (msgCounter.get() < batchSize) {
                msg = consumer.receiveNoWait();
            }
        } catch (JMSException ex) {
            LOG.info("Exception when trying to receive message: " + ex.getMessage());
        }
        if (msg != null) {
            LOG.debug("sending tuple: %s ", msg.toString());
            // get the tuple from the handler
            try {
                Values vals = tupleProducer.toTuple(msg);
                if (vals == null) {
                    pendingMessages.put(msg.getJMSMessageID(), msg);
                    msgCounter.incrementAndGet();
                    ack(msg.getJMSMessageID());
                    return;
                }
                // ack if we're not in AUTO_ACKNOWLEDGE mode, or the message requests ACKNOWLEDGE
                LOG.debug("Requested deliveryMode: " + toDeliveryModeString(msg.getJMSDeliveryMode()));
                LOG.debug("Our deliveryMode: " + toDeliveryModeString(jmsAcknowledgeMode));
                if (isDurableSubscription()
                        || (msg.getJMSDeliveryMode() != Session.AUTO_ACKNOWLEDGE)) {
                    LOG.debug("Requesting acks.");
                    collector.emit(vals, msg.getJMSMessageID());

					// at this point we successfully emitted. Store
                    // the message and message ID so we can do a
                    // JMS acknowledge later
                    pendingMessages.put(msg.getJMSMessageID(), msg);
                    msgCounter.incrementAndGet();
                } else {
                    collector.emit(vals);
                }
            } catch (JMSException e) {
                LOG.warn("Unable to convert JMS message: " + msg);
            }
        }
    }

    /*
     * Will only be called if we're transactional or not AUTO_ACKNOWLEDGE
     */
    @Override
    public void ack(Object _msgId) {
        processBatch(_msgId);
    }

    /*
     * Will only be called if we're transactional or not AUTO_ACKNOWLEDGE
     */
    @Override
    public void fail(Object _msgId) {
        LOG.warn("Message failed: " + _msgId);
        Message msg = pendingMessages.get(_msgId);
        if (msg != null) {
            try {
                if (msg.getJMSRedelivered()) {
                    // poisoned message protection:
                    // this is the second failure -> expire
                    // the message and send back to the queue
                    // depending on the broker used the queue can be configured to move 
                    // these messages in a designated queue.
                    // the original message will be properly acknowledged
                    // TODO: make this behaviour configurable
                    Session producerSession = connection.createSession(false, jmsAcknowledgeMode);
                    try {
                        MessageProducer producer = producerSession.createProducer(msg.getJMSDestination());
                        producer.send(msg, msg.getJMSDeliveryMode(), msg.getJMSPriority(), 1);
                        producer.close();
                    } finally {
                        producerSession.close();
                    }
                } else {
                    hasFailures = true;
                }
            } catch (JMSException ex) {
                LOG.error(null, ex);
            }
        }
        processBatch(_msgId);
    }

    private void processBatch(Object _msgId) {
        Message msg = pendingMessages.remove(_msgId);

        if (msg != null) {
            // only acknowledge or recover when full batch has been processed
            if (msgCounter.get() >= batchSize && pendingMessages.isEmpty()) {
                if (!hasFailures()) {
                    try {
                        msg.acknowledge();
                        LOG.debug("JMS Message acked: " + _msgId);
                    } catch (JMSException e) {
                        LOG.warn("Error acknowledging JMS message: " + _msgId, e);
                        doRecover();
                    }
                } else {
                    doRecover();
                }
                msgCounter.set(0);
            }
        } else {
            LOG.warn("Unknown JMS message ID: " + _msgId);
        }
    }

    private void doRecover() {
        LOG.info("Recovering session from a message failure.");
        try {
            getSession().recover();
            recovered();
        } catch (JMSException ex) {
            LOG.warn("Could not recover jms session.", ex);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer _declarer) {
        tupleProducer.declareOutputFields(_declarer);

    }

    /**
     * Returns <code>true</code> if the spout has received failures from which it has not yet recovered.
     */
    protected boolean hasFailures() {
        return hasFailures;
    }

    protected void recovered() {
        hasFailures = false;
    }

    public boolean isDistributed() {
        return distributed;
    }

    /**
     * Sets the "distributed" mode of this spout.
     * <p/>
     * If <code>true</code> multiple instances of this spout <i>may</i> be created across the cluster (depending on the
     * "parallelism_hint" in the topology configuration).
     * <p/>
     * Setting this value to <code>false</code> essentially means this spout will run as a singleton within the cluster
     * ("parallelism_hint" will be ignored).
     * <p/>
     * In general, this should be set to <code>false</code> if the underlying JMS destination is a topic, and
     * <code>true</code> if it is a JMS queue.
     *
     * @param _distributed
     */
    public void setDistributed(boolean _distributed) {
        distributed = _distributed;
    }

    private static final String toDeliveryModeString(int _deliveryMode) {
        switch (_deliveryMode) {
            case Session.AUTO_ACKNOWLEDGE:
                return "AUTO_ACKNOWLEDGE";
            case Session.CLIENT_ACKNOWLEDGE:
                return "CLIENT_ACKNOWLEDGE";
            case Session.DUPS_OK_ACKNOWLEDGE:
                return "DUPS_OK_ACKNOWLEDGE";
            default:
                return "UNKNOWN";

        }
    }

    protected Session getSession() {
        return session;
    }

    private boolean isDurableSubscription() {
        return (jmsAcknowledgeMode != Session.AUTO_ACKNOWLEDGE);
    }

}
