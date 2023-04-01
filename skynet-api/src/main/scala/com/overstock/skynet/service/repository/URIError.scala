package com.overstock.skynet.service.repository

import cats.data.NonEmptyList
import cats.kernel.Semigroup

import java.net.URI

sealed trait URIError extends Error with Product with Serializable {
  def input: URI
}

object URIError {
  final case class Cause(input: URI, repo: String, cause: Throwable) extends URIError {
    override def getMessage: String = s"$repo: URI error for '$input': ${cause.getMessage}"
  }

  final case class Reason(input: URI, repo: String, reason: String) extends URIError {
    override def getMessage: String = s"$repo: URI error for '$input': $reason"
  }

  final case class Multiple(input: URI, errors: NonEmptyList[URIError]) extends URIError {
    override def getMessage: String = s"Multiple URI errors \n ${errors.map(_.getMessage).toList.mkString("\n")}"
  }

  implicit val semigroupURIParseError: Semigroup[URIError] = Semigroup.instance {
    case (Multiple(input, errors), Multiple(_, b)) => Multiple(input, errors concatNel b)
    case (Multiple(input, errors), other) => Multiple(input, errors append other)
    case (other, Multiple(input, errors)) => Multiple(input, errors prepend other)
    case (a, b) => Multiple(a.input, NonEmptyList(a, List(b)))
  }
}
