package com.twitter.stitch;

import com.twitter.util.Await;
import com.twitter.util.Function;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class JavaTest {

  @Test
  public void testValue() throws Exception {
    assertEquals(
      Await.result(Stitch.run(Stitch.value("foo"))),
      "foo");
  }

  @Test
  public void testJoin() throws Exception {
    assertEquals(
      Await.result(Stitch.run(Stitch.join(
        Stitch.value("foo"),
        Stitch.value("bar")))),
      new scala.Tuple2<String, String>("foo", "bar"));
  }

  @Test
  public void testCollect() throws Exception {
    assertEquals(
      Await.result(Stitch.run(Stitch.collect(Arrays.asList(Stitch.value("foo"), Stitch.value("bar"))))),
      Arrays.asList("foo", "bar"));
  }

  @Test
  public void testTraverse() throws Exception {
    assertEquals(
      Await.result(Stitch.run(Stitch.traverse(
        Arrays.asList("foo", "bar"),
        new Function<String, Stitch<String>>() {
          public Stitch<String> apply(String s) { return Stitch.value(s.toUpperCase()); }
        }))),
      Arrays.asList("FOO", "BAR"));
  }

  @Test
  public void testMap() throws Exception {
    assertEquals(
      Await.result(Stitch.run(
        Stitch.value("foo").map(new Function<String, String>() {
            public String apply(String s) { return s.toUpperCase(); }
          }))),
      "FOO");
  }

  @Test
  public void testFlatMap() throws Exception {
    assertEquals(
      Await.result(Stitch.run(
        Stitch.value("foo").flatMap(new Function<String, Stitch<String>>() {
          public Stitch<String> apply(String s) { return Stitch.value(s.toUpperCase()); }
        }))),
      "FOO");
  }

  @Test
  public void testHandle() throws Exception {
    assertEquals(
      Await.result(Stitch.run(
        Stitch.exception(new Exception()).handle(new Function<Throwable, String>() {
          public String apply(Throwable t) { return "bar"; }
        }))),
      "bar");
  }

  @Test
  public void testRescue() throws Exception {
    assertEquals(
      Await.result(Stitch.run(
        Stitch.exception(new Exception()).rescue(new Function<Throwable, Stitch<String>>() {
          public Stitch<String> apply(Throwable t) { return Stitch.value("bar"); }
        }))),
      "bar");
  }

  @Test
  public void testCallFuture() throws Exception {
    assertEquals(
      Await.result(Stitch.run(Stitch.callFuture(new Function0<Future<String>>() {
        public Future<String> apply() { return Future.value("foo"); }
      }))),
      "foo");
  }
}
