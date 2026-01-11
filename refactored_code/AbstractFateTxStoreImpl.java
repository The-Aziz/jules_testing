package org.apache.accumulo.core.fate;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.fate.Fate.TxInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public abstract class AbstractFateTxStoreImpl<T> implements FateTxStore<T> {
  private static final Logger log = LoggerFactory.getLogger(AbstractFateTxStoreImpl.class);

  protected final FateId fateId;
  protected final boolean isReserved;
  protected TStatus observedStatus = null;
  private final AbstractFateStore<T> store;

  protected AbstractFateTxStoreImpl(FateId fateId, boolean isReserved, AbstractFateStore<T> store) {
    this.fateId = fateId;
    this.isReserved = isReserved;
    this.store = store;
  }

  @Override
  public TStatus waitForStatusChange(EnumSet<TStatus> expected) {
    Preconditions.checkState(!isReserved,
        "Attempted to wait for status change while reserved " + fateId);
    while (true) {
      long countBefore = store.unreservedNonNewCount.getCount();
      TStatus status = store._getStatus(fateId);
      if (expected.contains(status)) {
        return status;
      }
      store.unreservedNonNewCount.waitFor(count -> count != countBefore, 1000, () -> true);
    }
  }

  @Override
  public void unreserve(long deferTime, TimeUnit timeUnit) {
    deferTime = TimeUnit.NANOSECONDS.convert(deferTime, timeUnit);
    if (deferTime < 0) {
      throw new IllegalArgumentException("deferTime < 0 : " + deferTime);
    }
    synchronized (store) {
      if (!store.reserved.remove(fateId)) {
        throw new IllegalStateException("Tried to unreserve id that was not reserved " + fateId);
      }
      store.notifyAll();
      if (deferTime > 0 && !store.isDeferredOverflow()) {
          if (store.getDeferredCount() >= store.getMaxDeferred()) {
              log.info(
                      "Deferred map overflowed with size {}, clearing and setting deferredOverflow to true",
                      store.getDeferredCount());
              store.setDeferredOverflow(true);
              store.clearDeferred();
          } else {
              store.defer(fateId, System.nanoTime() + deferTime);
          }
      }
    }
    if (observedStatus != null && store.isRunnable(observedStatus)) {
      store.unreservedRunnableCount.increment();
    }
    if (observedStatus != TStatus.NEW) {
      store.unreservedNonNewCount.increment();
    }
  }

  protected void verifyReserved(boolean isWrite) {
    if (!isReserved && isWrite) {
      throw new IllegalStateException("Attempted write on unreserved FATE transaction.");
    }
    if (isReserved) {
      synchronized (store) {
        if (!store.reserved.contains(fateId)) {
          throw new IllegalStateException("Tried to operate on unreserved transaction " + fateId);
        }
      }
    }
  }

  @Override
  public TStatus getStatus() {
    verifyReserved(false);
    var status = store._getStatus(fateId);
    observedStatus = status;
    return status;
  }

  @Override
  public FateId getID() {
    return fateId;
  }

  protected byte[] serializeTxInfo(Serializable so) {
    if (so instanceof String) {
      return ("S " + so).getBytes(UTF_8);
    } else {
      byte[] sera = AbstractFateStore.serialize(so);
      byte[] data = new byte[sera.length + 2];
      System.arraycopy(sera, 0, data, 2, sera.length);
      data[0] = 'O';
      data[1] = ' ';
      return data;
    }
  }

  protected Serializable deserializeTxInfo(TxInfo txInfo, byte[] data) {
    if (data[0] == 'O') {
      byte[] sera = new byte[data.length - 2];
      System.arraycopy(data, 2, sera, 0, sera.length);
      return (Serializable) AbstractFateStore.deserialize(sera);
    } else if (data[0] == 'S') {
      return new String(data, 2, data.length - 2, UTF_8);
    } else {
      throw new IllegalStateException("Bad node data " + txInfo);
    }
  }
}
