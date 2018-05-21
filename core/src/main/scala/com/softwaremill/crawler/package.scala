package com.softwaremill

package object crawler {
  type Domain = String
  case class Url(domain: Domain, path: String)
}
