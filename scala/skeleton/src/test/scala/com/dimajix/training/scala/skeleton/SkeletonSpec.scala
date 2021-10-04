package com.dimajix.training.scala.skeleton

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfter


class SkeletonSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {
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
