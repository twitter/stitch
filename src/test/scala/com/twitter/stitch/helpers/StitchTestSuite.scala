package com.twitter.stitch.helpers

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

/** A base class for Stitch test suites */
abstract class StitchTestSuite extends AnyFunSuite with Matchers with AwaitHelpers
