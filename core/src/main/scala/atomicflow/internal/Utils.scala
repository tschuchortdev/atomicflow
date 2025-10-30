package atomicflow.internal

extension [A](a: A)
  private[atomicflow] def catching(f: PartialFunction[Throwable, A]) = try a catch f