package example

import scala.collection.mutable.ListBuffer
import java.io._
import scala.io.Source
import java.util.Properties
import scala.util.Random
import java.util.UUID
import scala.collection.JavaConverters._
import scala.util._
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object genData {

    val random = new Random()
    val country = "United States of America"
    val namesFile = "/home/maria_dev/names.txt"
    val statesFile = "/home/maria_dev/states.txt"
    val dateFile = "/home/maria_dev/date.txt"
    val csvFile = "/home/maria_dev/insurance.csv"
 
    def main(args: Array[String]):Unit = {
      createCSV()
    }

    def getFileLines(filePath: String): List[Any] = {
        val file = new File(filePath)//This is passed to the function as a paramater 
        val fileLines = Source.fromFile(file).getLines().toList // creates file contents as a list
        return fileLines // returns file contents as a list of type: Any if you want to use a diferent type it must be converted
    }

    def names(): String = {
        val nameList = getFileLines(namesFile) //returns a list of names using the getfilelines function
        val name = nameList(random.nextInt(nameList.length)).toString // randomly gets a name from the list
        // commented code is only if you want each name to have a unique id attached to it
        // val id = nameList.indexOf(name) 
        // val idName = s"$id,$name"
        return name
    }

    def age(): String = {
        val ageList = (20 to 90).toList // 20-90 as a list
        var age = ageList(random.nextInt(ageList.length)).toString // randomomly selects a num in the list
        return age
    }

    def state(): String = {
        val filePath = statesFile
        val file = new File(filePath)
        val stateList = Source.fromFile(file).getLines().toList
        val state = stateList(random.nextInt(stateList.length))
        return state
    }

    def date(): String = {
        val filePath = dateFile
        val file = new File(filePath)
        val dateList = Source.fromFile(file).getLines().toList
        val date = dateList(random.nextInt(dateList.length))
        return date
    }

    def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
      try { f(param) } finally { param.close() }//this function closes files after writing

    def writeToFile(fileName:String, data:String) = 
      using (new FileWriter(fileName)) 
      {
        fileWriter => fileWriter.write(data)//simple write function will over write the contents of a file
      }

    def appendToFile(fileName:String, textData:String) =
      using (new FileWriter(fileName, true)){ 
      fileWriter => using (new PrintWriter(fileWriter)) {
        printWriter => printWriter.println(textData)// this is the appending funtion and will not overwrite a file
      }
    }

    def id(): String = {
      val randID = UUID.randomUUID().toString() // gives rand uuid 
      return randID
    }

    def amount() : String = {
        val amountList = (500 to 30000).toList
        var amount = amountList(random.nextInt(amountList.length)).toString
        return amount
    }

    def claimCat() : String = {
        val categoryList = List( "Dental", "Vision", "Medical", "Life", "Natural Disaster")
        var category = categoryList(random.nextInt(categoryList.length)).toString()


        return category
    }

    def reasonCC(claimCat : String, state: String) : String = {
      
      val dentalReasons = List("Teeth cleaning","Cavity", "Braces", "Dental Xrays")
      val lifeReasons = List("Fatal Traffic Accident", "Death", "Terrorist Attack", "Fatal Heart Attack", "Accidental", "Suicide")
      val visonReasons = List("New glasses","Eye exam", "New Contacts", "Lazer Eye Surgery")
      val medicalReasons = List("Health Check Up", "Broken Bone", "Flu diagnosis", "Vaccinations")
      val naturalDisasterReasons = List("Fire", "Flood", "Hurricane", "Tornado", "Earthquake")


      if (claimCat == "Dental") {
        val dent = dentalReasons(random.nextInt(dentalReasons.length)).toString()
        return dent
      }
      else if(claimCat == "Vision"){
        val vis = visonReasons(random.nextInt(visonReasons.length)).toString()
        return vis
      }
      else if(claimCat == "Medical"){
        val med = medicalReasons(random.nextInt(medicalReasons.length)).toString()
        return med
      }
      else if(claimCat == "Natural Disater"){
        if(state == "Flordia"){
          val hurricane = "Hurricane"
          return hurricane
        }
        else{
          val nat = naturalDisasterReasons(random.nextInt(naturalDisasterReasons.length)).toString()
          return nat
        }
        
      }
      else {
        val el = lifeReasons(random.nextInt(lifeReasons.length)).toString()
        return el
      }  
    }
    //dwaynes id function
  //   def agentid(): String = {
  //     val agentsid = (1 to 10).toList
  //     var iD = agentsid(random.nextInt(agentsid.length)).toString()
  //     return iD
  // }

    def agentNameId(): String = {
      val nameList = List("Michael","Christopher","Jessica","Matthew","Ashley","Jennifer","Joshua","Amanda","Daniel","David")
      var name = nameList(random.nextInt(nameList.length)).toString()// randomly gets a name from the list
      val id = nameList.indexOf(name)//gets the index of the random name in the list
      val idName = s"${id+1},$name" //creates a string with the id+1 so that there is no 0 id and then the name the id is attached to
      return idName
    }

    def agentRating(): String = {
      val ratingList = (1 to 10).toList // 1-10 as a list
      val rating = ratingList(random.nextInt(ratingList.length)).toString() // randomomly selects a num in the list
      return rating
    }

    def approval(): String = {
      val approvalList = List("Y", "N")
      val approval = approvalList(random.nextInt(approvalList.length)).toString()// randomomly selects a Y/N in the list
      return approval
    }

    def failureReason(claimCat: String, approval: String): String = {
      val denialReasons = List("Not covered by policy","Doctor Not in Coverage Network", "Expired insurance", "deductible Not Met")
      val lifeReasons = List("policy not in effect during claim", "non payment of premium", "death ruled suicide")    
      if(approval == "N") // checks to see what if the claim was approved or not
        {
          if (claimCat == "Life") 
            {
              val lifeR = lifeReasons(random.nextInt(lifeReasons.length)).toString() // if its a life insurance case then randomly seleces and item in the lifereasons list
              return lifeR
            }
          else 
            {
              val denialR = denialReasons(random.nextInt(denialReasons.length)).toString()// if its not a life insurance case then randomly seleces and item in the denialreasons list 
              return denialR
            }
        } 
      else
        {
          val noReason = "NULL" // if it is approved returns nothing
          return noReason
        }
      }
    def createCSV(): Unit = {
      val insData = csvFile
      //val feilds = "claim_id,customer_id,customer_name,Customer_age,agent_id,agent_name,claim_category,amount,reason,agent_rating,datetime,country,state,approval,reimbursement_id,failure_reason\n"
      writeToFile(insData, "")
      println("Creating Data")
      for(i <- 1 until 5000) //for loop to determine how big to make data set
      {
        val claim = claimCat() //claim paramater to pass to reasonCC/falure reason
        val approvalIs = approval()//aapproval paramater to pass to falure reason
        println(s"Creating Data: ${i + 1}") // prints the count of as data is being created
        val data = id() + "," + id() + "," + names() + "," + age() + "," + agentNameId() + "," + claim + "," + amount() + "," + reasonCC(claim) + ","  + agentRating() + "," + date() + "," + country + "," + state() + "," + approvalIs + "," + id() + "," + failureReason(claim,approvalIs) 
        appendToFile(insData, data)
      }
      }
}

object KafkaProducerApp {
  def main_kp(args: Array[String]): Unit = {
    val props: Properties = new Properties()
    //props.put("bootstrap.servers","localhost:9092")
    props.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
    props.put(
      "key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    props.put(
      "value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    // The acks config controls the criteria under which requests are considered complete.
    // The "all" setting we have specified will result in blocking on the full commit of the record,
    // the slowest but most durable setting.
    props.put("acks", "all")
    val producer = new KafkaProducer[String, String](props)
    val topic = "text_topic"
    try {
      for (i <- 0 to 15) {
        val record = new ProducerRecord[String, String](
          topic,
          i.toString,
          "This is item #" + i
        )
        val metadata = producer.send(record)
        printf(
          s"sent record(key=%s value=%s) " +
            "meta(partition=%d, offset=%d)\n",
          record.key(),
          record.value(),
          metadata.get().partition(),
          metadata.get().offset()
        )
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      producer.close()
    }
  }
}

