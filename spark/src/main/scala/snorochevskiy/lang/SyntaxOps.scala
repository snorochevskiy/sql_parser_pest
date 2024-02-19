package snorochevskiy.lang

object SyntaxOps {
  implicit class IdOps[A](val self: A) extends AnyVal {
    def |>[B](f: A=>B): B = f(self)
    def some: Some[A] = Some(self)
    def opt: Option[A] = Option(self)
  }
}

object OptionOps {
  implicit class OptTuple2[A,B](val self: (Option[A], Option[B])) extends AnyVal {
    def app[C](f: (A,B)=>C): Option[C] =
      for {
        a <- self._1
        b <- self._2
      } yield f(a, b)
  }
}
