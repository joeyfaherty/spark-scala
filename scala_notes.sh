# best way to inverse sort in scala
list.sortBy(_.size)(Ordering[Int].reverse)
