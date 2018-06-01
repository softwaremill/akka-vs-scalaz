package com.softwaremill

package object crawler {
  type Host = String
  case class Url(host: Host, path: String)
}
