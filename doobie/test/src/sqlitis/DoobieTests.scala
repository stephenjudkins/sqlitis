package sqlitis
import utest._
import cats.effect._
import cats.implicits._
import _root_.doobie._
import _root_.doobie.implicits._
import _root_.doobie.h2._
import cats.effect.internals.{IOAppPlatform, IOContextShift}
import sqlitis.Query.{Column, Ctx, Ref, Table, Q}
import sqlitis.doobie.Doobie
import shapeless._
import scala.io.Source

object DoobieTests extends TestSuite {
  protected implicit def contextShift: ContextShift[IO] =
    IOContextShift.global

  def transactor: Resource[IO, H2Transactor[IO]] =
    for {
      ce <- ExecutionContexts.fixedThreadPool[IO](32) // our connect EC
      te <- ExecutionContexts.cachedThreadPool[IO]    // our transaction EC
      xa <- H2Transactor.newH2Transactor[IO](
        "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", // connect URL
        "sa",                                   // username
        "",                                     //   password
        ce,                                     // await connection here
        te                                      // execute JDBC operations here
      )
    } yield xa



  def schema = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("test.sql")).mkString


  case class Person[C <: Ctx](
    id: C#HasDefault[Int],
    name: C#NoDefault[String],
    age: C#NoDefault[Int]
  )

  implicit object Person extends Table[Person] {
    val name = "person"
    val schema = Person[Ctx.Schema](
      id = Column("id"),
      name = Column("name"),
      age = Column("age")
    )
  }

  case class Pet[C <: Ctx](
    id: C#HasDefault[Int],
    name: C#NoDefault[String],
    age: C#NoDefault[Int],
    personId: C#NoDefault[Int]
  )

  implicit object Pet extends Table[Pet] {
    val name = "pet"
    val schema = Pet[Ctx.Schema](
      id = Column("id"),
      name = Column("name"),
      age = Column("age"),
      personId = Column("person_id")
    )
  }

  val query:Q[(Ref[String], Ref[String])] = for {
    person <- Query[Person]
    pet <- Query[Pet] if person.id === pet.id
  } yield (person.name, pet.name)


  def tests = Tests {
    'basicQuery - {

      val io = transactor.use { xa =>
        for {
          _ <- _root_.doobie.Update(schema).run(()).transact(xa)
          r <- Doobie.select(query).transact(xa)
        } yield r
      }

      assert(io.unsafeRunSync() == List(
        ("Charlie Brown", "Snoopy"),
        ("Calvin", "Hobbes")
      ))
    }
  }
}
