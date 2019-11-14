package io.titandata.remote.s3.server

import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import io.titandata.remote.RemoteServer
import io.titandata.remote.RemoteServerUtil
import java.io.File
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission

/**
 * The S3 provider is a very simple provider for storing whole commits directly in a S3 bucket. Each commit is is a
 * key within a folder, for example:
 *
 *      s3://bucket/path/to/repo/3583-4053-598ea-298fa
 *
 * Within each commit sub-directory, there is .tar.gz file for each volume. The metadata for each commit is stored
 * as metadata for the object, as well in a 'titan' file at the root of the repository, with once line per commit. We
 * do this for a few reasons:
 *
 *      * Storing it in object metdata is inefficient, as there's no way to fetch the metadata of multiple objects
 *        at once. We keep it per-object for the cases where we
 *      * We want to be able to access this data in a read-only fashion over the HTTP interface, and there is no way
 *        to access object metadata (or even necessarily iterate over objects) through the HTTP interface.
 *
 * This has its downsides, namely that deleting a commit is more complicated, and there is greater risk of
 * concurrent operations creating invalid state, but those are existing challenges with these simplistic providers.
 * Properly solving them would require a more sophisticated provider with server-side logic.
 */
class S3RemoteServer : RemoteServer {

    internal val gson = GsonBuilder().create()
    internal val util = RemoteServerUtil()

    override fun getProvider(): String {
        return "s3"
    }

    /**
     * This function will return the (bucket, key) that identifies the given commit (or root key if no commit
     * is specified). This takes into the account the optional path configured in the remote.
     */
    internal fun getPath(remote: Map<String, Any>, commitId: String? = null): Pair<String, String?> {
        val key = if (remote["path"] == null) {
            commitId
        } else if (commitId == null) {
            remote["path"] as String
        } else {
            "${remote["path"]}/$commitId"
        }

        return Pair(remote["bucket"] as String, key)
    }


    override fun getCommit(remote: Map<String, Any>, parameters: Map<String, Any>, commitId: String): Map<String, Any>? {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun listCommits(remote: Map<String, Any>, parameters: Map<String, Any>, tags: List<Pair<String, String?>>): List<Pair<String, Map<String, Any>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}
