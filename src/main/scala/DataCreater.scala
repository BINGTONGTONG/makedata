package main.scala.CreateData



import java.io.{FileWriter, Writer}

import scala.util.Random

/**
* Created by Zhao Qiang on 2016/12/8.
*/
object DataCreater {

def main(args: Array[String]): Unit = {
val datapath = "./datatmp/platform.txt"
val max_records = args(0).toInt
val age = 70
val brand = Array("Huawei","MI","Apple","Samsung","Meizu","Lenovo","Oppo","Nokia")
val rand = new Random()
val writer: FileWriter = new FileWriter(datapath,true)

// create age of data
for(i <- 1 to max_records){
var dataage = rand.nextInt(age)
if (dataage < 15){dataage = age + 15}

//create phonePlus of data
var phonePlus = brand(rand.nextInt(8))

//create clicks of data
var clicks = rand.nextInt(20)

//create users of data
var name = "Role"+ rand.nextInt(100).toString
//println(name)

var months = rand.nextInt(12)+1
var logintime = "2016" + "-" + months + "-" + rand.nextInt(31)
//println(logintime)

//DataStructure("ID","Username","Userage","PhoneType,"Click","LoginTime")
writer.write(i + "," + name + "," + dataage + "," + phonePlus + "," + clicks + "," + logintime)
writer.write(System.getProperty("line.separator"))
}
writer.flush()
writer.close()
System.exit(1)
}
}
