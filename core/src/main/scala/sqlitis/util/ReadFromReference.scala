package sqlitis.util

trait ReadFromReference[A] {
  def apply: A
}
