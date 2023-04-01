package com.overstock.skynet.service

import cats.data.Validated
import zio.Has
package object repository {

  type Checked[URI] = Validated[URIError, URI]

  type Repository = Has[Repository.Service]
}
