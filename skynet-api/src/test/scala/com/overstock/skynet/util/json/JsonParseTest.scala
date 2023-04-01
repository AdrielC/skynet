package com.overstock.skynet.util.json

import com.overstock.skynet.domain.Frame
import ml.combust.mleap.runtime.frame.DefaultLeapFrame
import org.scalatest.FlatSpec

class JsonParseTest extends FlatSpec {

  lazy val leapFrameJson = {
    """{
      |    "schema": {
      |        "fields": [
      |            {
      |                "name": "a",
      |                "type": {
      |                    "type": "basic",
      |                    "base": "double",
      |                    "isNullable": false
      |                }
      |            },
      |            {
      |                "name": "b",
      |                "type": "double"
      |            },
      |            {
      |                "name": "c",
      |                "type": "double"
      |            },
      |            {
      |                "name": "d",
      |                "type": {
      |                    "type": "list",
      |                    "base": "string"
      |                }
      |            },
      |            {
      |                "name": "e",
      |                "type": "double"
      |            },
      |            {
      |                "name": "f",
      |                "type": {
      |                    "type": "list",
      |                    "base": "string"
      |                }
      |            },
      |            {
      |                "name": "g",
      |                "type": {
      |                    "type": "list",
      |                    "base": "string"
      |                }
      |            },
      |            {
      |                "name": "h",
      |                "type": "double"
      |            },
      |            {
      |                "name": "i",
      |                "type": "double"
      |            },
      |            {
      |                "name": "j",
      |                "type": "double"
      |            },
      |            {
      |                "name": "k",
      |                "type": {
      |                    "type": "list",
      |                    "base": "string"
      |                }
      |            },
      |            {
      |                "name": "l",
      |                "type": {
      |                    "type": "list",
      |                    "base": "string"
      |                }
      |            },
      |            {
      |                "name": "m",
      |                "type": "double"
      |            },
      |            {
      |                "name": "n",
      |                "type": "double"
      |            },
      |            {
      |                "name": "o",
      |                "type": "double"
      |            },
      |            {
      |                "name": "p",
      |                "type": "double"
      |            },
      |            {
      |                "name": "q",
      |                "type": "string"
      |            },
      |            {
      |                "name": "r",
      |                "type": {
      |                    "type": "list",
      |                    "base": "string"
      |                }
      |            },
      |            {
      |                "name": "s",
      |                "type": "double"
      |            },
      |            {
      |                "name": "t",
      |                "type": "double"
      |            },
      |            {
      |                "name": "u",
      |                "type": "double"
      |            },
      |            {
      |                "name": "v",
      |                "type": "double"
      |            },
      |            {
      |                "name": "w",
      |                "type": "double"
      |            },
      |            {
      |                "name": "x",
      |                "type": "double"
      |            },
      |            {
      |                "name": "y",
      |                "type": {
      |                    "type": "list",
      |                    "base": "string"
      |                }
      |            },
      |            {
      |                "name": "z",
      |                "type": "double"
      |            },
      |            {
      |                "name": "aa",
      |                "type": "double"
      |            },
      |            {
      |                "name": "bb",
      |                "type": "double"
      |            },
      |            {
      |                "name": "cc",
      |                "type": "double",
      |                "isNullable": false
      |            },
      |            {
      |                "name": "dd",
      |                "type": {
      |                    "type": "basic",
      |                    "base": "string",
      |                    "isNullable": false
      |                }
      |            },
      |            {
      |                "name": "ee",
      |                "type": "double"
      |            },
      |            {
      |                "name": "ff",
      |                "type": {
      |                    "type": "list",
      |                    "base": "string"
      |                }
      |            },
      |            {
      |                "name": "gg",
      |                "type": "double"
      |            },
      |            {
      |                "name": "hh",
      |                "type": "double"
      |            },
      |            {
      |                "name": "ii",
      |                "type": "double"
      |            },
      |            {
      |                "name": "jj",
      |                "type": "double"
      |            },
      |            {
      |                "name": "kk",
      |                "type": "string"
      |            },
      |            {
      |                "name": "ll",
      |                "type": {
      |                    "type": "basic",
      |                    "base": "string",
      |                    "isNullable": false
      |                }
      |            },
      |            {
      |                "name": "mm",
      |                "type": "double"
      |            }
      |        ]
      |    },
      |    "rows": [
      |        [
      |            0.3157792317771507,
      |            0.11958191952850417,
      |            0.34334627299589415,
      |            [
      |                "",
      |                "",
      |                "qP",
      |                "da4",
      |                "",
      |                "",
      |                "j~VOd'3[D",
      |                "u"
      |            ],
      |            0.07038295792521432,
      |            [
      |                "9C>E{}taV",
      |                ""
      |            ],
      |            [],
      |            0.23002162299203244,
      |            0.22431773561218327,
      |            0.17442124214451393,
      |            [
      |                "",
      |                "bz",
      |                ":0IimGj",
      |                "1efK7;~\"",
      |                "M\\Ye,]yjD",
      |                "^7S^iZ"
      |            ],
      |            [
      |                "KG\"YpWis<",
      |                "qIsW-\"V",
      |                "",
      |                "G",
      |                "",
      |                "Z-;P",
      |                ""
      |            ],
      |            0.15067226881821072,
      |            0.5448752835171524,
      |            0.9953732557022967,
      |            0.9533529289632994,
      |            "",
      |            [
      |                "B`((~r",
      |                "",
      |                "",
      |                "Q*w6\\<.fK",
      |                "",
      |                "bl",
      |                "",
      |                "4iXn9;)qk"
      |            ],
      |            0.31375630761504236,
      |            0.01826560233202945,
      |            0.21468813627316474,
      |            0.5906026940512424,
      |            0.33731622305175,
      |            0.9666114634848306,
      |            [
      |                "",
      |                "",
      |                "",
      |                "5M-?0<k7",
      |                "rMJ0qI",
      |                ""
      |            ],
      |            0.5849358569330886,
      |            0.7238508293534773,
      |            0.08678159425345999,
      |            0.802724655306557,
      |            "",
      |            0.529787219317607,
      |            [
      |                "Ny:5i.zT$",
      |                "",
      |                "UC",
      |                "",
      |                "%.PVt(V"
      |            ],
      |            0.7802461989458277,
      |            0.8174076000893109,
      |            0.4889900779606715,
      |            0.8654161077411625,
      |            "Fb+m;",
      |            "",
      |            0.648840230880087
      |        ]
      |    ]
      |}""".stripMargin
  }

  it should "parse leapframe" in {
    import io.circe.parser._
    import ml.combust.mleap.json.circe._

    val frame = decode[DefaultLeapFrame](leapFrameJson)

    frame.toTry.get.printSchema()
  }

  val frameJson =
    """
      |{
      |    "contextFrame": {
      |        "schema": {
      |            "fields": [
      |            {
      |                "name": "a",
      |                "type": "string"
      |            },
      |            {
      |                "name": "b",
      |                "type": "double"
      |            },
      |            {
      |                "name": "c",
      |                "type": "double"
      |            },
      |            {
      |                "name": "d",
      |                "type": "string"
      |            },
      |            {
      |                "name": "e",
      |                "type": "double"
      |            },
      |            {
      |                "name": "f",
      |                "type": {
      |                "type": "list",
      |                "base": "string"
      |                }
      |            },
      |            {
      |                "name": "g",
      |                "type": {
      |                "type": "list",
      |                "base": "string"
      |                }
      |            },
      |            {
      |                "name": "h",
      |                "type": "double"
      |            },
      |            {
      |                "name": "i",
      |                "type": "double"
      |            },
      |            {
      |                "name": "j",
      |                "type": "double"
      |            },
      |            {
      |                "name": "k",
      |                "type": "double"
      |            },
      |            {
      |                "name": "l",
      |                "type": "double"
      |            },
      |            {
      |                "name": "m",
      |                "type": {
      |                "type": "list",
      |                "base": "string"
      |                }
      |            },
      |            {
      |                "name": "n",
      |                "type": {
      |                "type": "list",
      |                "base": "string"
      |                }
      |            },
      |            {
      |                "name": "o",
      |                "type": {
      |                  "type": "list",
      |                  "base": "string"
      |                }
      |            },
      |            {
      |                "name": "p",
      |                "type": {
      |                  "type": "list",
      |                  "base": "string"
      |                }
      |            }
      |            ]
      |        },
      |        "rows": [
      |            [
      |                "192485054392",
      |                348,
      |                184.5,
      |                "asdf",
      |                348,
      |                [],
      |                ["appear","throughout:","from","the"],
      |                5,
      |                588,
      |                0.47768376028639176,
      |                304,
      |                0,
      |                ["asdf", "qwer"],
      |                [],
      |                ["ha","asdf","dfg","dsag","sdfg","asdf"],
      |                ["sdfgsdfgdfg"]
      |            ]
      |        ]
      |    },
      |
      |    "targetFrame": {
      |
      |        "schema": {
      |
      |            "fields": [
      |                {
      |                    "name": "q",
      |                    "type": "long"
      |                },
      |                {
      |                    "name": "r",
      |                    "type": "long"
      |                },
      |                {
      |                    "name": "s",
      |                    "type": "long"
      |                },
      |                {
      |                    "name": "t",
      |                    "type": "long"
      |                },
      |                {
      |                    "name": "u",
      |                    "type": "double"
      |                },
      |                {
      |                    "name": "v",
      |                    "type": "long"
      |                },
      |                {
      |                    "name": "w",
      |                    "type": "long"
      |                },
      |                {
      |                    "name": "x",
      |                    "type": "long"
      |                },
      |                {
      |                    "name": "y",
      |                    "type": "long"
      |                },
      |                {
      |                    "name": "z",
      |                    "type": "double"
      |                },
      |                {
      |                    "name": "aa",
      |                    "type": "string"
      |                },
      |                {
      |                    "name": "bb",
      |                    "type": "double"
      |                },
      |                {
      |                    "name": "cc",
      |                    "type": "double"
      |                },
      |                {
      |                    "name": "dd",
      |                    "type": "double"
      |                },
      |                {
      |                    "name": "ee",
      |                    "type": "double"
      |                },
      |                {
      |                    "name": "ff",
      |                    "type": "string"
      |                },
      |                {
      |                    "name": "gg",
      |                    "type": "double"
      |                },
      |                {
      |                    "name": "hh",
      |                    "type": {
      |                    "type": "list",
      |                    "base": "string"
      |                    }
      |                },
      |                {
      |                    "name": "ii",
      |                    "type": "double"
      |                },
      |                {
      |                    "name": "jj",
      |                    "type": "double"
      |                },
      |                {
      |                    "name": "kk",
      |                    "type": {
      |                    "type": "list",
      |                    "base": "string"
      |                    }
      |                },
      |                {
      |                    "name": "ll",
      |                    "type": "double"
      |                },
      |                {
      |                    "name": "mm",
      |                    "type": "double"
      |                }
      |            ]
      |        },
      |
      |        "rows": [
      |            [
      |                0,
      |                0,
      |                496,
      |                0,
      |                238,
      |                0,
      |                0,
      |                101,
      |                0,
      |                1364.9,
      |                "asdf",
      |                0,
      |                0,
      |                39202,
      |                0,
      |                "asdfadfasdf",
      |                119,
      |                [
      |                    ""
      |                ],
      |                0.12354498347852208,
      |                0.021825247822168818,
      |                [
      |                    ""
      |                ],
      |                0.012652415693076884,
      |                0.0025763991633079943
      |            ]
      |        ]
      |    }
      |}
      |""".stripMargin

  it should "parse leapframe with longs" in {

    import io.circe.parser._

    val frame = decode[Frame](frameJson)

    println(frameJson)

    println(frame.toTry.get.flatten.get)
  }
}
