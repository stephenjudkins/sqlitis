package sqlitis

import shapeless.Id
import sqlitis.Query.{Column, Ref}

trait Ctx {
  type NoDefault[_]
  type HasDefault[_]
}

object Ctx {
  type AllColumns[F[_]] = Ctx {
    type NoDefault[X]  = F[X]
    type HasDefault[X] = F[X]
  }

  type Schema     = AllColumns[Column]
  type Queried[X] = AllColumns[Ref[X, *]]
  type Concrete   = AllColumns[Id]
  type Inserted = Ctx {
    type NoDefault[X]  = X
    type HasDefault[X] = Option[X]
  }
}
