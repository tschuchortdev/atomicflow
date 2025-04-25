package atomicflow

trait Cacheable[A] {

}

object Cacheable {
  given [A] => Cacheable[A] = new Cacheable[A] {}
}
