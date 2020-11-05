package sqlitis.doobie

import doobie.free.connection.ConnectionIO
import doobie.util.Read
import doobie.util.query.Query
import sqlitis.Generator
import sqlitis.Query._

object Doobie {
  def select[A, O](q: Q[Unit, A])(implicit
      resultExtractor: ResultExtractor[A, O, Unit],
      read: Read[O]
  ): ConnectionIO[List[O]] =
    Query[Unit, O](
      Generator.GenSelect.print(q.run(resultExtractor).sql)
    ).to[List](())

}
