package snorochevskiy.util

import java.io.Closeable

object Bracket {

  def withResource[A <: AnyRef,B](init: => A, close: A => Unit)(f: A => B): B = {
    val a = init
    try {
      f(a)
    } finally {
      if (a ne null) close(a)
    }
  }

  def withCloseable[A <: Closeable, B](init: => A)(f: A => B): B = {
    val a = init
    try {
      f(a)
    } finally {
      if (a ne null) a.close()
    }
  }

  def withCloseable2[A <: Closeable, B <: Closeable, C](init1: => A)(init2: A => B)(f: B => C): C =
    withCloseable(init1) { a =>
      withCloseable(init2(a)) { b =>
        f(b)
      }
    }

  def withBiCloseable[A <: Closeable, B <: Closeable, C](initA: => A, initB: => B)(f: (A,B) => C): C = {
    val a = initA
    val b = initB
    try {
      f(a,b)
    } finally {
      if (a ne null) a.close()
      if (b ne null) b.close()
    }
  }

}
