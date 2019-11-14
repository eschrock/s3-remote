package io.titandata.remote.s3.server

import io.kotlintest.TestCase
import io.kotlintest.TestResult
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.mockk.MockKAnnotations
import io.mockk.Runs
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.MockK
import io.mockk.impl.annotations.OverrideMockKs
import io.mockk.impl.annotations.SpyK
import io.mockk.just
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.slot
import io.mockk.verify
import java.io.File
import java.lang.IllegalArgumentException
import java.nio.file.Files

class S3RemoteServerTest : StringSpec() {

    @SpyK
    var client = S3RemoteServer()

    override fun beforeTest(testCase: TestCase) {
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    init {
        "get path returns bucket" {
            val (bucket) = client.getPath(mapOf("bucket" to "bucket"))
            bucket shouldBe "bucket"
        }

        "get path returns commit ID if path not specified" {
            val result = client.getPath(mapOf("bucket" to "bucket"), "id")
            result.second shouldBe "id"
        }

        "get path returns path if commit ID not set" {
            val result = client.getPath(mapOf("bucket" to "bucket", "path" to "key"))
            result.second shouldBe "key"
        }

        "get path returns path plus commit ID if set" {
            val result = client.getPath(mapOf("bucket" to "bucket", "path" to "key"), "id")
            result.second shouldBe "key/id"
        }
    }
}
