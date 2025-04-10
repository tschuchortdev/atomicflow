object Test {
  def decompressFile(bytes: Array[Byte]): Array[Byte] = ???

  def decryptFile(bytes: Array[Byte], senderIk: String): Array[Byte] = ???

  def sendFile(bytes: Array[Byte], receiver: String) = ???

  trait StepCache {

  }

  trait StepCtx {

  }

  def waitFor[A](using ctx: StepCtx): A = {
    ???
  }

  object step {
    trait ArgCtx[T[_]]

    trait Result[A] {
      def get[T[_]](using ArgCtx[T]): T[A]
    }

    type ResultsOf[T <: Tuple] = Tuple.Map[T, Result]

    type UnwrapResult[T] = T match
      case Result[a] => a

    type UnwrapResultTuple[T <: Tuple] = Tuple.InverseMap[T, Result]

    type Id[A] = A
    
    trait CtxFn[Outer[_[_]]] {
      type O = Outer

      def apply[F[_]](using ArgCtx[F]): Outer[F]
    }
    
    def simple0[Out](name: String, version: Long)()(f: => Out): Result[Out] = ???

    def step[In[_[_]], Out](name: String, version: Long)(args: [F[_]] => ArgCtx[F] ?=> In[F])(f: In[Id] => Out): Result[Out] = ???

    def step2[In[_[_]], Out](name: String, version: Long)(args: CtxFn[?])(f: args.O[Id] => Out): Result[Out] = ???

    def cached[In <: Tuple, Out](using StepCache)(name: String, version: Long)(args: In)(f: Tuple.InverseMap[In, Result] => Out): Result[Out] = ???

    def once[In <: Tuple, Out](using StepCache)(name: String, version: Long)(args: In)(f: In => Out): Result[Out] = ???
  }

  // executable multiple times
  // executable once

  case class Step[In, Out](name: String, version: Long) {
    def apply(f: In => Out): Out = ???

    def apply(using In =:= Unit)(f: => Out): Out = ???
  }

  type A = (String, Int)
  type B = Tuple.Map[A, Option]

  def wf = {
    val str = if (true) "asdf" else "aaa"
    val result1: step.Result[String] = step.simple0("first step", 0)() {
      "a"
    }


    val result2: step.Result[Int] = step.simple0("second step", 0)() {
      1
    }

    def func(arg: [A] => A => A) = ???
    
    func([A] => a => a)

    given StepCache = new StepCache {}
    
    val result3 = step.cached("third step", 0)((result1, result2)) { (a, b) =>
      a.trim
      "a"
    }
    val firstStep = Step[String, Array[Byte]]("first", 0)
    firstStep { input =>
      decompressFile(Array.empty)
    }
  }
}