/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.metamx.tranquility.kafka;


import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.metamx.common.logger.Logger;
import com.metamx.tranquility.config.DataSourceConfig;
import com.metamx.tranquility.kafka.model.MessageCounters;
import com.metamx.tranquility.kafka.model.PropertiesBasedKafkaConfig;
import com.metamx.tranquility.kafka.writer.WriterController;
import io.druid.concurrent.Execs;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Consumer
{
  private static final Logger log = new Logger(KafkaConsumer.class);

  private final ExecutorService consumerExec;
  private final Thread commitThread;
  private final AtomicBoolean shutdown = new AtomicBoolean();

  private final ReentrantReadWriteLock commitLock = new ReentrantReadWriteLock();

  private final Pattern topicFilter;
  private final int numThreads;
  private final int commitMillis;
  private final WriterController writerController;

  private Map<String, MessageCounters> previousMessageCounters = new HashMap<>();

  private final Properties kafkaProperties;
  private Set<KafkaConsumer> polledConsumers;
  private Set<KafkaConsumer> consumerSet;

  public Consumer(
      final PropertiesBasedKafkaConfig globalConfig,
      final Properties kafkaProperties,
      final Map<String, DataSourceConfig<PropertiesBasedKafkaConfig>> dataSourceConfigs,
      final WriterController writerController
  )
  {
    this.kafkaProperties = (Properties) kafkaProperties.clone();;
    Preconditions.checkState(
        !kafkaProperties.getProperty("enable.auto.commit", "").equals("true"),
        "autocommit must be off"
    );
    this.kafkaProperties.setProperty("enable.auto.commit", "false");
    this.polledConsumers = Collections.synchronizedSet(new HashSet<KafkaConsumer>());
    this.consumerSet = Collections.synchronizedSet(new HashSet<KafkaConsumer>());
    this.topicFilter = Pattern.compile(buildTopicFilter(dataSourceConfigs));

    log.info("Kafka topic filter [%s]", this.topicFilter.pattern());

    int defaultNumThreads = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
    this.numThreads = globalConfig.getConsumerNumThreads() > 0
                      ? globalConfig.getConsumerNumThreads()
                      : defaultNumThreads;

    this.commitMillis = globalConfig.getCommitPeriodMillis();
    this.writerController = writerController;
    this.consumerExec = Execs.multiThreaded(this.numThreads, "KafkaConsumer-%d");
    this.commitThread = new Thread(createCommitRunnable());
    this.commitThread.setName("KafkaConsumer-CommitThread");
    this.commitThread.setDaemon(true);
  }

  public void start()
  {
    commitThread.start();
    startConsumers();
  }

  public void stop()
  {
    if (shutdown.compareAndSet(false, true)) {
      log.info("Shutting down - attempting to flush buffers and commit final offsets");

      try {
        commitLock.writeLock().lockInterruptibly(); // prevent Kafka from consuming any more events
        try {
          writerController.flushAll(); // try to flush the remaining events to Druid
          writerController.stop();
          Iterator<KafkaConsumer> consumerIterator = polledConsumers.iterator();
          while (consumerIterator.hasNext()) {
              KafkaConsumer kafkaConsumer = consumerIterator.next();
              kafkaConsumer.commitSync();
              consumerIterator.remove();
          }
        }
        finally {
          for (KafkaConsumer consumer : consumerSet) {
            consumer.close();
          }
          commitLock.writeLock().unlock();
          commitThread.interrupt();
          consumerExec.shutdownNow();
        }
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        Throwables.propagate(e);
      }

      log.info("Finished clean shutdown.");
    }
  }

  public void join() throws InterruptedException
  {
    commitThread.join();
  }

  void commit() throws InterruptedException
  {
    commitLock.writeLock().lockInterruptibly();
    try {
      final long flushStartTime = System.currentTimeMillis();
      final Map<String, MessageCounters> messageCounters = writerController.flushAll(); // blocks until complete

      final long commitStartTime = System.currentTimeMillis();

      Iterator<KafkaConsumer> consumerIterator = polledConsumers.iterator();
      while (consumerIterator.hasNext()) {
          KafkaConsumer kafkaConsumer = consumerIterator.next();
          kafkaConsumer.commitSync();
          consumerIterator.remove();
      }

      final long finishedTime = System.currentTimeMillis();
      Map<String, MessageCounters> countsSinceLastCommit = new HashMap();
      for (Map.Entry<String, MessageCounters> entry : messageCounters.entrySet()) {
        countsSinceLastCommit.put(
            entry.getKey(),
            entry.getValue().difference(previousMessageCounters.get(entry.getKey()))
        );
      }

      previousMessageCounters = messageCounters;

      log.info(
          "Flushed %s pending messages in %sms and committed offsets in %sms.",
          countsSinceLastCommit.isEmpty() ? "0" : countsSinceLastCommit,
          commitStartTime - flushStartTime,
          finishedTime - commitStartTime
      );
    }
    finally {
      commitLock.writeLock().unlock();
    }
  }

  private Runnable createCommitRunnable()
  {
    return new Runnable()
    {
      @Override
      public void run()
      {
        long lastFlushTime = System.currentTimeMillis();
        try {
          while (!Thread.currentThread().isInterrupted()) {
            Thread.sleep(Math.max(commitMillis - (System.currentTimeMillis() - lastFlushTime), 0));
            commit();
            lastFlushTime = System.currentTimeMillis();
          }
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          log.info("Commit thread interrupted.");
        }
        catch (Throwable e) {
          log.error(e, "Commit thread failed!");
          throw Throwables.propagate(e);
        }
        finally {
          stop();
        }
      }
    };
  }

  private class NoopConsumerRebalanceListener implements ConsumerRebalanceListener {
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
  }

  private void startConsumers()
  {
    consumerExec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaProperties);
              kafkaConsumer.subscribe(topicFilter, new NoopConsumerRebalanceListener());
              consumerSet.add(kafkaConsumer);

              while(true) {
                if (Thread.currentThread().isInterrupted()) {
                  throw new InterruptedException();
                }

                // In order to guarantee at-least-once message delivery, we need to a) set autocommit enable to 
                // false so the consumer will not automatically commit offsets to Kafka, and b) synchronize calls of 
                // kafkaConsumer.poll() with the commit thread so that we don't read messages and then call 
                // kafkaConsumer.commitSync() before those messages have been flushed through Tranquility into 
                // the indexing service.
                commitLock.readLock().lockInterruptibly();

                try {
                  ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                  for (ConsumerRecord<String, String> record : records) {
                    writerController.getWriter(record.topic()).send(record.value().getBytes());
                  }
                  if (!records.isEmpty()) {
                    polledConsumers.add(kafkaConsumer);
                  }
                }
                finally {
                  commitLock.readLock().unlock();
                }
              }
            }
            catch (InterruptedException e) {
              log.info("Consumer thread interrupted.");
            }
            catch (Throwable e) {
              log.error(e, "Exception: ");
              throw Throwables.propagate(e);
            }
            finally {
              stop();
            }
          }
        }
    );
  }

  private static String buildTopicFilter(Map<String, DataSourceConfig<PropertiesBasedKafkaConfig>> dataSourceConfigs)
  {
    StringBuilder topicFilter = new StringBuilder();
    for (Map.Entry<String, DataSourceConfig<PropertiesBasedKafkaConfig>> entry : dataSourceConfigs.entrySet()) {
      topicFilter.append(String.format("(%s)|", entry.getValue().propertiesBasedConfig().getTopicPattern()));
    }

    return topicFilter.length() > 0 ? topicFilter.substring(0, topicFilter.length() - 1) : "";
  }
}
