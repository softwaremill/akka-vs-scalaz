package com.softwaremill

import cats.Monad
import scalaz.effect.IO

object IOInstances {
  implicit def ioMonad[E]: Monad[IO[E, ?]] = new Monad[IO[E, ?]] {
    override def flatMap[A, B](fa: IO[E, A])(f: A => IO[E, B]): IO[E, B] = fa.flatMap(f)

    override def tailRecM[A, B](a: A)(f: A => IO[E, Either[A, B]]): IO[E, B] =
      IO.suspend[E, Either[A, B]](f(a)).flatMap {
        case Left(continueA) => tailRecM(continueA)(f)
        case Right(b)        => IO.point(b)
      }

    override def pure[A](x: A): IO[E, A] = IO.point(x)

    override def map[A, B](fa: IO[E, A])(f: A => B): IO[E, B] = fa.map(f)
  }
}
