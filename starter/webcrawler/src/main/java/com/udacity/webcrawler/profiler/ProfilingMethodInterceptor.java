package com.udacity.webcrawler.profiler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.time.Instant;

/**
 * A method interceptor that checks whether {@link Method}s are annotated with the {@link Profiled}
 * annotation. If they are, the method interceptor records how long the method invocation took.
 */
final class ProfilingMethodInterceptor implements InvocationHandler {

  private final Clock clock;

  // TODO: You will need to add more instance fields and constructor arguments to this class.
  private final Object delegate;
  private final ProfilingState state;
  private final ZonedDateTime startTime;
  ProfilingMethodInterceptor(Clock clock, Object delegate, ProfilingState state, ZonedDateTime startTime) {
    this.clock = Objects.requireNonNull(clock);
    this.delegate = delegate;
    this.state = state;
    this.startTime = startTime;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    // TODO: This method interceptor should inspect the called method to see if it is a profiled
    //       method. For profiled methods, the interceptor should record the start time, then
    //       invoke the method using the object that is being profiled. Finally, for profiled
    //       methods, the interceptor should record how long the method call took, using the
    //       ProfilingState methods.
    Instant invocationStartTime = null;
    Object result = null;

    // Check if the method is annotated with @Profiled
    if (method.isAnnotationPresent(Profiled.class)) {
      invocationStartTime = clock.instant(); // Record the start time of the method invocation
    }
    try {
      // Invoke the method using the delegate object
      result = method.invoke(delegate, args);
    } catch (InvocationTargetException ex) {
      throw ex.getTargetException();
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } finally {
      // Record the method execution time if it is a profiled method
      if (invocationStartTime != null) {
        Duration executionTime = Duration.between(invocationStartTime, clock.instant()); // Calculate the execution time
        state.record(delegate.getClass(), method, executionTime); // Record the method execution time in the profiling state
      }
    }
    return result; // Return the result of the method invocation
  }
}
