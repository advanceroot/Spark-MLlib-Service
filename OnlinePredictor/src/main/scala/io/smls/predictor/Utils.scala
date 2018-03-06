package io.smls.predictor

import java.net.NetworkInterface


object Utils {
  /**
    * Get ip address of the network interface.
    *
    * @param targetInterface
    * @return
    */
  def queryLocalIPAddress(targetInterface:String):String = {
    val interfaces = NetworkInterface.getNetworkInterfaces

    while(interfaces.hasMoreElements) {
      val net = interfaces.nextElement()
      val netName = net.getName

      if (netName == targetInterface){
        println(s"Find interface:${netName}")
        val addresses = net.getInetAddresses
        while (addresses.hasMoreElements) {
          val address = addresses.nextElement.getHostAddress
          println(s"${targetInterface}'s address -->"+address)
          if(!address.contains(":")){ //skip IPv6
            return address
          }
        }
      }
    }
    return null
  }
}
