package backtype.storm.contrib.jms.spout;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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

    private static final AtomicInteger THREAD_ID = new AtomicInteger();

    /** time in ms after a batch is committed, reagrdless of the number if messages consumed */
    private final long commitIntervalMs;

    private int batchSize;

    // JMS options
    private int jmsAcknowledgeMode = Session.CLIENT_ACKNOWLEDGE;

    private boolean distributed = true;

    private JmsTupleProducer tupleProducer;

    private JmsProvider jmsProvider;

    private SpoutOutputCollector collector;

    private transient ExecutorService executorService;

    private transient Consumer consumer;

    private transient Future<?> consumerFuture;


    public SynchronousJmsSpout() {
        this(DEFAULT_BATCH_SIZE);
    }
    
    /**
     * Constructs a <code>SynchronousJmsSpout</code>
     * @param _batchSize Defines the batch size to be used.
     */
    public SynchronousJmsSpout(int _batchSize) {
        this(_batchSize, 2, TimeUnit.SECONDS);
    }

    public SynchronousJmsSpout(int _batchSize, final long _commitInterval, final TimeUnit _commitIntervalUnit) {
        batchSize = _batchSize;
        commitIntervalMs = _commitIntervalUnit.toMillis(_commitInterval);
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
        consumerFuture = executorService.submit(consumer);
    }

    @Override
    public void deactivate() {
        super.deactivate();

        consumer.terminate();
        consumerFuture.cancel(true);

        consumer = null;
    }


    @Override
    public void nextTuple() {
        if (consumer != null) {
            final JMSValues jmsValues = consumer.nextTuple();
            if (jmsValues != null) {
                if (jmsAcknowledgeMode == Session.CLIENT_ACKNOWLEDGE) {
                    collector.emit(jmsValues.values, jmsValues.messageId);
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
            consumer.ack((MessageId) _msgId);
        }
    }

    /*
     * Will only be called if we're transactional or not AUTO_ACKNOWLEDGE
     */
    @Override
    public void fail(final Object _msgId) {
        LOG.warn("message failed: {}", _msgId);
        if (consumer != null) {
            consumer.fail((MessageId) _msgId);
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
        final MessageId messageId;
        final Values values;

        JMSValues(final MessageId _messageId, final Values _values) throws JMSException {
            messageId = _messageId;
            values = _values;
        }
    }

    private class Consumer implements Runnable, ExceptionListener {

        private transient Connection jmsConnection;

        private transient Session jmsSession;

        private transient MessageConsumer jmsConsumer;

        private int msgCount = 0;

        private boolean lastBatchFailed = false;

        private final AtomicLong batchId = new AtomicLong(0);

        private final Set<MessageId> pending = new HashSet<MessageId>();

        private final AtomicBoolean failed = new AtomicBoolean(false);

        private final AtomicBoolean foreRestart = new AtomicBoolean(false);

        private final AtomicBoolean shutdown = new AtomicBoolean(false);

        private final ReentrantLock lock = new ReentrantLock();

        private final Condition msgResponseReceived = lock.newCondition();

        private final List<MessageId> acked = new ArrayList<MessageId>(batchSize);

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

                    batchId.incrementAndGet();
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
                        && msgCount < batchSize) {

                final long timeLeftMs = commitIntervalMs - (System.currentTimeMillis() - batchStartTs);
                if (timeLeftMs <= 0) {
                    break;
                }

                //
                // try to fetch the next message
                //
                Message message;
                try {
                    message = jmsConsumer.receive(timeLeftMs);
                } catch (final JMSException e) {
                    LOG.warn("error consuming message - recovering session: {}", e.getMessage());
                    failed.set(true);
                    break;
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

                        final MessageId messageId = new MessageId(batchId.get(), message.getJMSMessageID());
                        final JMSValues values = new JMSValues(messageId, tupleProducer.toTuple(message));

                        if (values.values != null) {
                            pending.add(messageId);
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

            if (LOG.isDebugEnabled()) {
                LOG.debug("finished receiving batch of {} msgs after {} ms", msgCount,
                        (System.currentTimeMillis() - batchStartTs));
            }

            //
            // wait until
            //   * outgoing queue is empty
            //   * we're having no pending messages
            //   * or we have a failure
            //
            lock.lock();
            try {
                while (!shutdown.get() && !failed.get() && !queue.isEmpty()) {
                    msgResponseReceived.await(5, TimeUnit.SECONDS);
                }

                while (!failed.get()) {
                    for (final MessageId messageId : acked) {
                        pending.remove(messageId);
                    }
                    acked.clear();

                    if (shutdown.get()) {
                        break;
                    }

                    if (pending.isEmpty()) {
                        break;
                    } else {
                        msgResponseReceived.await(5, TimeUnit.SECONDS);
                    }
                }
            } catch (final InterruptedException e) {
                LOG.info("recovering batch due to an interrupt");
                try {
                    if (pending.isEmpty() && lastCommitableMessage != null) {
                        lastCommitableMessage.acknowledge();
                    } else {
                        jmsSession.recover();
                    }
                } catch (final JMSException jmsException) {
                    LOG.error("failed to recover JMS session: {}", jmsException.getMessage());
                }
                throw e;
            } finally {
                lock.unlock();
            }
            
            if (LOG.isDebugEnabled()) {
        	    LOG.debug("after respionse receive wait batch of {} msgs after {} ms", msgCount, (System.currentTimeMillis() - batchStartTs));
            }

            //
            // finish batch
            //
            try {
                if (failed.get() || (shutdown.get() && !pending.isEmpty())) {
                    if (foreRestart.get()) {
                        restartConsumer();
                    } else {
                        jmsSession.recover();
                    }
                } else if (lastCommitableMessage != null) {
                    lastCommitableMessage.acknowledge();
                } else if (lastBatchFailed) {
                    LOG.warn("didn't got any messages after recovering from an error - restarting JMS connection");
                    restartConsumer();
                }
            } catch (final JMSException e) {
                LOG.error("failed to finish batch: {}", e.getMessage());
                restartConsumer();
            }

            if (LOG.isDebugEnabled()) {
        	    LOG.debug("acked batch of {} msgs after {} ms", msgCount, (System.currentTimeMillis() - batchStartTs));
            }

            queue.clear();
            pending.clear();
            msgCount = 0;

            lastBatchFailed = failed.getAndSet(false);
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
                    LOG.debug("closing JMS connection.");
                    jmsConnection.close();
                    jmsConnection = null;
                    jmsConsumer = null;
                    jmsSession = null;
                } catch (final JMSException e) {
                    LOG.warn("error closing JMS connection.", e);
                }
            }
        }

        private void restartConsumer() {
            foreRestart.set(false);
            close();
            Utils.sleep(150);
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

        void ack(final MessageId _messageId) {
            if (_messageId.batchId != batchId.get()) {
                LOG.info("batch id mismatch - ignored to ack message '{}', current batch id is {}",
                        _messageId, batchId.get());
                return;
            }
            try {
                lock.lockInterruptibly();
            } catch (final InterruptedException e) {
                failed.set(true);
                LOG.info("failed to ack JMS message '{}' due to an interrupt", _messageId);
                return;
            }
            try {
                acked.add(_messageId);
                msgResponseReceived.signalAll();
            } finally {
                lock.unlock();
            }
        }

        void fail(final MessageId _messageId) {
            if (_messageId.batchId != batchId.get()) {
                LOG.info("batch id mismatch - ignored to fail message '{}', current batch id is {}",
                        _messageId, batchId.get());
                return;
            }
            try {
                lock.lockInterruptibly();
            } catch (final InterruptedException e) {
                failed.set(true);
                LOG.info("failed to notify JMS consumer to recover after a failed JMS message '{}' due to an interrupt", _messageId);
                return;
            }
            try {
                failed.set(true);
                queue.clear();
                msgResponseReceived.signalAll();
            } finally {
                lock.unlock();
            }
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
                queue.clear();
                msgResponseReceived.signalAll();
            } finally {
                lock.unlock();
            }
        }


    }

    private static class MessageId implements Serializable {
        private final long batchId;
        private final String jmsMessageID;

        private MessageId(final long _batchId, final String _jmsMessageID) {
            batchId = _batchId;
            jmsMessageID = _jmsMessageID;
        }

        @Override
        public String toString() {
            return "MessageId{" +
                    "batchId=" + batchId +
                    ", jmsMessageID='" + jmsMessageID + '\'' +
                    '}';
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final MessageId messageId = (MessageId) o;

            if (batchId != messageId.batchId) {
                return false;
            }
            if (!jmsMessageID.equals(messageId.jmsMessageID)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (batchId ^ (batchId >>> 32));
            result = 31 * result + jmsMessageID.hashCode();
            return result;
        }
    }
}
