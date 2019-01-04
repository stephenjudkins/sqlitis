package sqlitis.doobie

import doobie.free.connection.ConnectionIO
import doobie.util.Read
import doobie.util.query.Query
import sqlitis.Generator
import sqlitis.Query.SelectResult

object Doobie {
  def select[A: Read](r: SelectResult[A]):ConnectionIO[List[A]] =
    Query[Unit, A](Generator.GenSelect.print(r.sql)).to[List](())
}
