package org.apache.accumulo.core.fate;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.accumulo.core.fate.Fate.TxInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public abstract class AbstractFateStore<T> implements FateStore<T> {

  private static final Logger log = LoggerFactory.getLogger(AbstractFateStore.class);

  // Default maximum size of 100,000 transactions before deferral is stopped and
  // all existing transactions are processed immediately again
  protected static final int DEFAULT_MAX_DEFERRED = 100_000;

  protected final Set<FateId> reserved;
  protected final Map<FateId,Long> deferred;
  private final int maxDeferred;
  private final AtomicBoolean deferredOverflow = new AtomicBoolean();

  // This is incremented each time a transaction was unreserved that was non new
  protected final SignalCount unreservedNonNewCount = new SignalCount();

  // This is incremented each time a transaction is unreserved that was runnable
  protected final SignalCount unreservedRunnableCount = new SignalCount();

  public AbstractFateStore() {
    this(DEFAULT_MAX_DEFERRED);
  }

  public AbstractFateStore(int maxDeferred) {
    this.maxDeferred = maxDeferred;
    this.reserved = new HashSet<>();
    this.deferred = new HashMap<>();
  }

  public static byte[] serialize(Object o) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(o);
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressFBWarnings(value = "OBJECT_DESERIALIZATION",
      justification = "unsafe to store arbitrary serialized objects like this, but needed for now"
          + " for backwards compatibility")
  public static Object deserialize(byte[] ser) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(ser);
        ObjectInputStream ois = new ObjectInputStream(bais)) {
      return ois.readObject();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Attempt to reserve the fate transaction.
   *
   * @param fateId The FateId
   * @return An Optional containing the FateTxStore if the transaction was successfully reserved, or
   *         an empty Optional if the transaction was already reserved.
   */
  @Override
  public Optional<FateTxStore<T>> tryReserve(FateId fateId) {
    synchronized (this) {
      if (!reserved.contains(fateId)) {
        return Optional.of(reserve(fateId));
      }
      return Optional.empty();
    }
  }

  @Override
  public FateTxStore<T> reserve(FateId fateId) {
    synchronized (AbstractFateStore.this) {
      while (reserved.contains(fateId)) {
        try {
          AbstractFateStore.this.wait(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }

      reserved.add(fateId);
      return newFateTxStore(fateId, true);
    }
  }

  @Override
  public void runnable(AtomicBoolean keepWaiting, Consumer<FateId> idConsumer) {

    AtomicLong seen = new AtomicLong(0);

    while (keepWaiting.get() && seen.get() == 0) {
      final long beforeCount = unreservedRunnableCount.getCount();
      final boolean beforeDeferredOverflow = deferredOverflow.get();

      try (Stream<FateIdStatus> transactions = getTransactions()) {
        transactions.filter(fateIdStatus -> isRunnable(fateIdStatus.getStatus()))
            .map(FateIdStatus::getFateId).filter(fateId -> {
              synchronized (AbstractFateStore.this) {
                var deferredTime = deferred.get(fateId);
                if (deferredTime != null) {
                  if ((deferredTime - System.nanoTime()) >= 0) {
                    return false;
                  } else {
                    deferred.remove(fateId);
                  }
                }
                return !reserved.contains(fateId);
              }
            }).forEach(fateId -> {
              seen.incrementAndGet();
              idConsumer.accept(fateId);
            });
      }

      // If deferredOverflow was previously marked true then the deferred map
      // would have been cleared and seen.get() should be greater than 0 as there would
      // be a lot of transactions to process in the previous run, so we won't be sleeping here
      if (seen.get() == 0) {
        if (beforeCount == unreservedRunnableCount.getCount()) {
          long waitTime = 5000;
          synchronized (AbstractFateStore.this) {
            if (!deferred.isEmpty()) {
              long currTime = System.nanoTime();
              long minWait =
                  deferred.values().stream().mapToLong(l -> l - currTime).min().getAsLong();
              waitTime = TimeUnit.MILLISECONDS.convert(minWait, TimeUnit.NANOSECONDS);
            }
          }

          if (waitTime > 0) {
            unreservedRunnableCount.waitFor(count -> count != beforeCount, waitTime,
                keepWaiting::get);
          }
        }
      }

      // Reset if the current state only if it matches the state before the execution.
      // This is to avoid a race condition where the flag was set during the run.
      // We should ensure at least one of the FATE executors will run through the
      // entire transaction list first before clearing the flag and allowing more
      // deferred entries into the map again. In other words, if the before state
      // was false and during the execution at some point it was marked true this would
      // not reset until after the next run
      deferredOverflow.compareAndSet(beforeDeferredOverflow, false);
    }
  }

  @Override
  public Stream<FateIdStatus> list() {
    return getTransactions();
  }

  @Override
  public ReadOnlyFateTxStore<T> read(FateId fateId) {
    return newFateTxStore(fateId, false);
  }

  protected boolean isRunnable(TStatus status) {
    return status == TStatus.IN_PROGRESS || status == TStatus.FAILED_IN_PROGRESS
        || status == TStatus.SUBMITTED;
  }

  public static abstract class FateIdStatusBase implements FateIdStatus {
    private final FateId fateId;

    public FateIdStatusBase(FateId fateId) {
      this.fateId = fateId;
    }

    @Override
    public FateId getFateId() {
      return fateId;
    }
  }

  @Override
  public boolean isDeferredOverflow() {
    return deferredOverflow.get();
  }

  @Override
  public int getDeferredCount() {
    // This method is primarily used right now for unit testing but
    // if this synchronization becomes an issue we could add an atomic
    // counter instead to track it separately so we don't need to lock
    synchronized (AbstractFateStore.this) {
      return deferred.size();
    }
  }

  protected abstract Stream<FateIdStatus> getTransactions();

  protected abstract TStatus _getStatus(FateId fateId);

  protected abstract FateTxStore<T> newFateTxStore(FateId fateId, boolean isReserved);

  protected int getMaxDeferred() {
    return maxDeferred;
  }

    protected void setDeferredOverflow(boolean value) {
        deferredOverflow.set(value);
    }

    protected void clearDeferred() {
        deferred.clear();
    }

    protected void defer(FateId fateId, long time) {
        deferred.put(fateId, time);
    }
}
