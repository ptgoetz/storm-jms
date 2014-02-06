package backtype.storm.contrib.jms.spout;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import backtype.storm.contrib.jms.JmsProvider;
import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * @author Carsten Krebs
 *
 */
@SuppressWarnings("serial")
public class SynchronousJmsSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(SynchronousJmsSpout.class);

    private static final int DEFAULT_BATCH_SIZE = 50;

    /** time in ms after a batch is committed, reagrdless of the number if messages consumed */
    private static final long ACK_INTERVAL_MS = 2000L;

    private static final AtomicInteger THREAD_ID = new AtomicInteger();

    private int batchSize;

    // JMS options
    private int jmsAcknowledgeMode = Session.CLIENT_ACKNOWLEDGE;

    private boolean distributed = true;

    private JmsTupleProducer tupleProducer;

    private JmsProvider jmsProvider;

    private SpoutOutputCollector collector;

    private transient ExecutorService executorService;

    private transient Consumer consumer;


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
            case Session.CLIENT_ACKNOWLEDGE:
                break;
            case Session.AUTO_ACKNOWLEDGE:
            case Session.DUPS_OK_ACKNOWLEDGE:
                throw new IllegalArgumentException("acknowledgmode is currently not implemented/supported!");
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
    public void open(final Map _conf, final TopologyContext _context, final SpoutOutputCollector _collector) {
        if (jmsProvider == null) {
            throw new IllegalStateException("JMS provider has not been set.");
        }
        if (tupleProducer == null) {
            throw new IllegalStateException("JMS Tuple Producer has not been set.");
        }

        collector = _collector;
        executorService = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(final Runnable _runnable) {
                final Thread thread = new Thread(_runnable);
                thread.setName("thread-jms-consumer-" + THREAD_ID.incrementAndGet() + "-" + jmsProvider.getName());
                return thread;
            }
        });
    }

    @Override
    public void close() {
        super.close();

        executorService.shutdown();
    }

    @Override
    public void activate() {
        super.activate();

        consumer = new Consumer();
        consumer.start();
        executorService.submit(consumer);
    }

    @Override
    public void deactivate() {
        super.deactivate();

        consumer.terminate();
        consumer = null;
    }


    @Override
    public void nextTuple() {
        if (consumer != null) {
            final JMSValues jmsValues = consumer.nextTuple();
            if (jmsValues != null) {
                if (jmsAcknowledgeMode == Session.CLIENT_ACKNOWLEDGE) {
                    collector.emit(jmsValues.values, jmsValues.jmsMessageID);
                } else {
                    collector.emit(jmsValues.values);
                }
            } else {
                Utils.sleep(3);
            }
        }
    }

    private void logDuration(final String _msg, final long _ts) {
        final long duration = System.currentTimeMillis() - _ts;
        if (duration > 10) {
            LOG.debug("{}: call took {} ms", _msg, duration);
        }
    }

    /*
     * Will only be called if we're transactional or not AUTO_ACKNOWLEDGE
     */
    @Override
    public void ack(final Object _msgId) {
        if (consumer != null) {
            consumer.ack((String) _msgId);
        }
    }

    /*
     * Will only be called if we're transactional or not AUTO_ACKNOWLEDGE
     */
    @Override
    public void fail(final Object _msgId) {
        LOG.warn("message failed: {}", _msgId);
        if (consumer != null) {
            consumer.recover();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer _declarer) {
        tupleProducer.declareOutputFields(_declarer);

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

    private static class JMSValues {
        final String jmsMessageID;
        final Values values;

        JMSValues(final String _jmsMessageID, final Values _values) throws JMSException {
            jmsMessageID = _jmsMessageID;
            values = _values;
        }
    }

    private class Consumer implements Runnable, ExceptionListener {

        private transient Connection jmsConnection;

        private transient Session jmsSession;

        private transient MessageConsumer jmsConsumer;

        private int msgCount = 0;

        private final Set<String> pending = new HashSet<String>();

        private final AtomicBoolean failed = new AtomicBoolean(false);

        private final AtomicBoolean foreRestart = new AtomicBoolean(false);

        private final AtomicBoolean shutdown = new AtomicBoolean(false);

        private final ReentrantLock lock = new ReentrantLock();

        private final Condition msgResponseReceived = lock.newCondition();

        private final List<String> acked = new ArrayList<String>(batchSize);

        private final ConcurrentLinkedQueue<JMSValues> queue = new ConcurrentLinkedQueue<JMSValues>();

        @Override
        public void run() {
            if (jmsConsumer == null) {
                throw new IllegalStateException("consumer is not started!");
            }

            try {
                while (!shutdown.get()) {
                    try {
                        receiveBatch();
                    } catch (final InterruptedException e) {
                        LOG.info("JMS consumer terminated due to an interrupt");
                        shutdown.set(true);
                        return;
                    }
                }
            } finally {
                close();
            }
        }

        private void receiveBatch() throws InterruptedException {
            final long batchStartTs = System.currentTimeMillis();

            Message lastCommitableMessage = null;
            while (!shutdown.get()
                        && !failed.get()
                        && msgCount < batchSize
                        && (System.currentTimeMillis() - batchStartTs) < ACK_INTERVAL_MS) {
                //
                // try to fetch the next message
                //
                Message message = null;
                try {
                    message = jmsConsumer.receive(100L);
                } catch (final JMSException e) {
                    LOG.warn("error consuming message: {}", e.getMessage());
                }

                //
                // fail-fast if any message failed
                //
                if (failed.get()) {
                    break;
                }

                //
                // add message to outgoing queue
                //
                if (message != null) {
                    try {
                        msgCount++;

                        final String jmsMessageID = message.getJMSMessageID();
                        final JMSValues values = new JMSValues(jmsMessageID, tupleProducer.toTuple(message));

                        if (values.values != null) {
                            pending.add(jmsMessageID);
                            queue.add(values);
                        }

                        lastCommitableMessage = message;
                    } catch (final JMSException e) {
                        LOG.error("Unable to convert JMS message: {}", message);
                        try {
                            moveToDMQ(message);
                        } catch (final JMSException e1) {
                            LOG.error("failed to moving message to the DMQ ({}): {}", message, e1.getMessage());
                            restartConsumer();
                        }
                    }
                }
            }

            //
            // wait until
            //   * outgoing queue is empty
            //   * we're having no pending messages
            //   * or we have a failure
            //
            lock.lock();
            try {
                while (!failed.get() && !queue.isEmpty()) {
                    msgResponseReceived.await(5, TimeUnit.SECONDS);
                }

                while (!failed.get()) {
                    for (final String jmsMessageID : acked) {
                        pending.remove(jmsMessageID);
                    }
                    acked.clear();

                    if (pending.isEmpty()) {
                        break;
                    } else {
                        msgResponseReceived.await(5, TimeUnit.SECONDS);
                    }
                }
            } catch (final InterruptedException e) {
                LOG.info("recovering batch due to an interrupt");
                try {
                    jmsSession.recover();
                } catch (final JMSException jmsException) {
                    LOG.error("failed to recover JMS session: {}", jmsException.getMessage());
                }
                throw e;
            } finally {
                lock.unlock();
            }

            //
            // finish batch
            //
            try {
                if (failed.get()) {
                    if (foreRestart.get()) {
                        restartConsumer();
                    } else {
                        jmsSession.recover();
                    }
                } else  if (lastCommitableMessage != null) {
                    lastCommitableMessage.acknowledge();
                }
            } catch (final JMSException e) {
                LOG.error("failed to finish batch: {}", e.getMessage());
                restartConsumer();
            }

            queue.clear();
            pending.clear();
            msgCount = 0;
            failed.set(false);
        }

        void start() {
            try {
                jmsConnection = jmsProvider.connectionFactory().createConnection();
                jmsConnection.setExceptionListener(this);
                try {
                    jmsSession = jmsConnection.createSession(false, jmsAcknowledgeMode);
                    jmsConsumer = jmsSession.createConsumer(jmsProvider.destination(), jmsProvider.messageSelector());
                    jmsConnection.start();
                } catch (final JMSException e1) {
                    jmsConnection.close();
                    jmsConnection = null;
                    jmsSession = null;
                    jmsConsumer = null;
                }
            } catch (final Exception e) {
                LOG.warn("Error creating JMS connection.", e);
            }
        }

        void close() {
            if (jmsConnection != null) {
                try {
                    LOG.debug("stopping JMS connection.");
                    jmsConnection.stop();
                } catch (final JMSException e) {
                    LOG.warn("error stopping JMS connection.", e);
                }

                try {
                    LOG.debug("closing JMS connection.");
                    jmsConnection.stop();
                } catch (final JMSException e) {
                    LOG.warn("error closing JMS connection.", e);
                }

                jmsConnection = null;
                jmsConsumer = null;
                jmsSession = null;
            }
        }

        private void restartConsumer() {
            foreRestart.set(false);
            close();
            start();
        }

        void terminate() {
            shutdown.set(true);
        }

        @Override
        public void onException(final JMSException _exception) {
            LOG.warn("got {} - restarting JMS consumer", _exception.getMessage());
            foreRestart.set(true);
            recover();
        }

        private void moveToDMQ(final Message _msg) throws JMSException {
            final Session producerSession = jmsConnection.createSession(false, jmsAcknowledgeMode);
            try {
                final MessageProducer producer = producerSession.createProducer(_msg.getJMSDestination());
                producer.send(_msg, _msg.getJMSDeliveryMode(), _msg.getJMSPriority(), 1);
                producer.close();
            } finally {
                producerSession.close();
            }
        }


        JMSValues nextTuple() {
            return queue.poll();
        }

        void recover() {
            try {
                lock.lockInterruptibly();
            } catch (final InterruptedException e) {
                LOG.info("failed to notify JMS consumer about revovering due to an interrupt");
                return;
            }
            try {
                failed.set(true);
                msgResponseReceived.signalAll();
            } finally {
                lock.unlock();
            }
        }

        void ack(final String _jmsMessageID) {
            try {
                lock.lockInterruptibly();
            } catch (final InterruptedException e) {
                failed.set(true);
                LOG.info("failed to ack JMS message '{}' due to an interrupt", _jmsMessageID);
                return;
            }
            try {
                acked.add(_jmsMessageID);
                msgResponseReceived.signalAll();
            } finally {
                lock.unlock();
            }
        }

    }

}
