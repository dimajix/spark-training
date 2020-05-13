package com.dimajix.training.scala.skeleton

import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}


class SkeletonSpec extends FlatSpec with Matchers with BeforeAndAfter {
    before {
        // Do stuff before
    }

    after {
        // Do stuff afterwards
    }

    "The simple things" should "work" in {
        1 + 1 should be (2)
    }
}
