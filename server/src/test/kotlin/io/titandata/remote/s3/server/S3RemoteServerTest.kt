package io.titandata.remote.s3.server

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.BasicSessionCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import io.kotlintest.TestCase
import io.kotlintest.TestCaseOrder
import io.kotlintest.TestResult
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.mockk.MockKAnnotations
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.impl.annotations.SpyK
import io.mockk.mockk
import io.mockk.mockkConstructor
import io.mockk.mockkStatic
import io.mockk.slot
import io.mockk.verify
import java.lang.IllegalArgumentException

class S3RemoteServerTest : StringSpec() {

    @SpyK
    var client = S3RemoteServer()

    override fun beforeTest(testCase: TestCase) {
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

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

        "get client fails with no access key" {
            shouldThrow<IllegalArgumentException> {
                client.getClient(mapOf("secretKey" to "secretKey", "region" to "region"), emptyMap())
            }
        }

        "get client fails with no secret key" {
            shouldThrow<IllegalArgumentException> {
                client.getClient(mapOf("accessKey" to "accessKey", "region" to "region"), emptyMap())
            }
        }

        "get client fails with no region" {
            shouldThrow<IllegalArgumentException> {
                client.getClient(mapOf("accessKey" to "accessKey", "secretKey" to "secretKey"), emptyMap())
            }
        }

        "get client uses basic session credentials" {
            mockkStatic(AmazonS3ClientBuilder::class)
            val builder = mockk<AmazonS3ClientBuilder>()
            every { AmazonS3ClientBuilder.standard() } returns builder
            every { builder.withRegion(any<String>()) } returns builder
            val slot = slot<AWSCredentialsProvider>()
            every { builder.withCredentials(capture(slot)) } returns builder
            every { builder.build() } returns mockk()

            client.getClient(mapOf("accessKey" to "accessKey", "secretKey" to "secretKey", "region" to "region"), emptyMap())

            val creds = slot.captured
            creds.credentials.awsAccessKeyId shouldBe "accessKey"
            creds.credentials.awsSecretKey shouldBe "secretKey"

            verify {
                builder.withRegion("region")
            }
        }

        "get client uses session token" {
            mockkStatic(AmazonS3ClientBuilder::class)
            val builder = mockk<AmazonS3ClientBuilder>()
            every { AmazonS3ClientBuilder.standard() } returns builder
            every { builder.withRegion(any<String>()) } returns builder
            val slot = slot<AWSCredentialsProvider>()
            every { builder.withCredentials(capture(slot)) } returns builder
            every { builder.build() } returns mockk()

            client.getClient(mapOf("accessKey" to "accessKey", "secretKey" to "secretKey", "region" to "region"), mapOf("sessionToken" to "token"))

            val creds = slot.captured
            creds.credentials.awsAccessKeyId shouldBe "accessKey"
            creds.credentials.awsSecretKey shouldBe "secretKey"
            (creds.credentials as BasicSessionCredentials).sessionToken shouldBe "token"

            verify {
                builder.withRegion("region")
            }
        }

        "get commit fails if no user metadata present" {
            val s3 : AmazonS3Client = mockk()
            every { s3.getObjectMetadata(any(), any()) } returns ObjectMetadata()
            every { client.getClient(any(), any()) } returns s3
            val result = client.getCommit(mapOf("bucket" to "bucket", "path" to "path"), emptyMap(), "id")
            result shouldBe null
            verify {
                s3.getObjectMetadata("bucket", "path/id")
            }
        }

        "get commit fails if metadata property is missing" {
            val metadata = ObjectMetadata()
            metadata.userMetadata = mapOf()
            val s3 : AmazonS3Client = mockk()
            every { s3.getObjectMetadata(any(), any()) } returns metadata
            every { client.getClient(any(), any()) } returns s3
            val result = client.getCommit(mapOf("bucket" to "bucket", "path" to "path"), emptyMap(), "id")
            result shouldBe null
        }

        "get commit fails if metadata is missing properties" {
            val metadata = ObjectMetadata()
            metadata.userMetadata = mapOf("io.titan-data" to "{}")
            val s3 : AmazonS3Client = mockk()
            every { s3.getObjectMetadata(any(), any()) } returns metadata
            every { client.getClient(any(), any()) } returns s3
            val result = client.getCommit(mapOf("bucket" to "bucket", "path" to "path"), emptyMap(), "id")
            result shouldBe null
        }

        "get commit succeeds" {
            val metadata = ObjectMetadata()
            metadata.userMetadata = mapOf("io.titan-data" to "{\"properties\":{\"a\":\"b\"}}")
            val s3 : AmazonS3Client = mockk()
            every { s3.getObjectMetadata(any(), any()) } returns metadata
            every { client.getClient(any(), any()) } returns s3
            val result = client.getCommit(mapOf("bucket" to "bucket", "path" to "path"), emptyMap(), "id")
            result shouldNotBe null
            result!!["a"] shouldBe "b"
        }
    }
}
