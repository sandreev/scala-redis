import org.specs._
import com.redis._

import org.specs.mock.Mockito
import org.mockito.Mock._
import org.mockito.Mockito._
import org.mockito.Mockito.doNothing

import org.specs.runner.JUnit4

class RedisClientSpecTest extends JUnit4(RedisClientSpec)
object RedisClientSpec extends Specification with Mockito {

  "Redis Client" should {
    var client: Redis = null
    
    "print formatted client status" in {
      client = new Redis("localhost", 121212)
      client.toString must be matching("localhost:121212 <connected:false>")
    }
    
    "get the same connection when passing key para or not since it's a single node" in {
      client = new Redis("localhost", 121212)
      client.getConnection("key") mustEqual client.getConnection
    }
    
    "use db zero as default" in {
      client = new Redis("localhost", 121212)
      client.db mustEqual 0
    }
  }
}
