package com.ericho.dropit

import com.ericho.dropit.model.FakeStorage
import com.ericho.dropit.model.FetchOptions
import io.github.cdimascio.dotenv.dotenv
import kotlinx.coroutines.runBlocking
import java.time.LocalDate
import java.time.format.DateTimeParseException
import kotlin.system.exitProcess

data class CliOptions(
    val deptConcurrency: Int = 2,
    val detailConcurrency: Int = 10,
    val resume: Boolean = false,
    val since: LocalDate? = null,
    val dryRun: Boolean = false,
    val baseUrl: String? = null,
    val token: String? = null,
    val out: String? = null,
)

fun main(args: Array<String>) = runBlocking {
    val dotenv = dotenv()
    val tempDir = dotenv["TEMP_FOLDER"] ?: System.getenv("TEMP_FOLDER") ?: "temp"
    val (options, command) = try {
        parseArgs(args)
    } catch (e: CliArgException) {
        System.err.println("Error: ${e.message}")
        System.err.println()
        printUsage()
        exitProcess(2)
    }

    if (command == null || command == "help") {
        printUsage()
        return@runBlocking
    }

    when (command) {
        "sync" -> {
            // TODO: create your real instances
            // val client = GroceryClient(baseUrl = options.baseUrl ?: "https://api.example.com", token = options.token)
            // val repo = GroceryRepository(client)
            // val service = SyncService(repo, storage, options.deptConcurrency, options.detailConcurrency, options.resume, options.since)

            println(
                """
                Running sync with:
                  deptConcurrency=${options.deptConcurrency}
                  detailConcurrency=${options.detailConcurrency}
                  resume=${options.resume}
                  since=${options.since}
                  dryRun=${options.dryRun}
                  baseUrl=${options.baseUrl}
                  out=${options.out}
                """.trimIndent()
            )
            val storage  = FakeStorage()
            val repo = GroceryRepository()
            val service = DropitFetchService(repo,storage)

            val fetchOptions = FetchOptions(
                deptConcurrency = options.deptConcurrency,
                detailConcurrency = options.detailConcurrency,
                resume = options.resume,
                since = options.since,
                dryRun = options.dryRun
            )
            val report = service.run(fetchOptions)
            println("Done. $report")
            // val report = service.syncAll(dryRun = options.dryRun, out = options.out)
            // println("Done. ${report}")
        }

        else -> {
            System.err.println("Unknown command: $command")
            System.err.println()
            printUsage()
            exitProcess(2)
        }
    }


}

/** ---------------- CLI parsing ---------------- */
private class CliArgException(msg: String) : RuntimeException(msg)

private fun parseArgs(args: Array<String>): Pair<CliOptions, String?> {
    if (args.isEmpty()) return CliOptions() to null

    var deptConcurrency = 2
    var detailConcurrency = 10
    var resume = false
    var since: LocalDate? = null
    var dryRun = false
    var baseUrl: String? = null
    var token: String? = null
    var out: String? = null

    var command: String? = null

    var i = 0
    while (i < args.size) {
        val a = args[i]

        // command: first non-flag token
        if (!a.startsWith("--") && command == null) {
            command = a
            i++
            continue
        }

        when (a) {
            "--help", "-h" -> return CliOptions() to "help"

            "--deptConcurrency" -> {
                deptConcurrency = args.requireIntAt(++i, "--deptConcurrency")
            }

            "--detailConcurrency" -> {
                detailConcurrency = args.requireIntAt(++i, "--detailConcurrency")
            }

            "--resume" -> resume = true

            "--since" -> {
                val raw = args.requireValueAt(++i, "--since")
                since = parseLocalDate(raw, "--since")
            }

            "--dryRun" -> dryRun = true

            "--baseUrl" -> {
                baseUrl = args.requireValueAt(++i, "--baseUrl")
            }

            "--token" -> {
                token = args.requireValueAt(++i, "--token")
            }

            "--out" -> {
                out = args.requireValueAt(++i, "--out")
            }

            else -> throw CliArgException("Unknown flag: $a")
        }
        i++
    }

    if (deptConcurrency <= 0) throw CliArgException("--deptConcurrency must be > 0")
    if (detailConcurrency <= 0) throw CliArgException("--detailConcurrency must be > 0")

    val options = CliOptions(
        deptConcurrency = deptConcurrency,
        detailConcurrency = detailConcurrency,
        resume = resume,
        since = since,
        dryRun = dryRun,
        baseUrl = baseUrl,
        token = token,
        out = out
    )
    return options to command
}

private fun Array<String>.requireValueAt(index: Int, flag: String): String {
    if (index !in indices) throw CliArgException("$flag requires a value")
    val v = this[index]
    if (v.startsWith("--")) throw CliArgException("$flag requires a value (got another flag: $v)")
    return v
}

private fun Array<String>.requireIntAt(index: Int, flag: String): Int {
    val v = requireValueAt(index, flag)
    return v.toIntOrNull() ?: throw CliArgException("$flag expects an integer, got: $v")
}

private fun parseLocalDate(value: String, flag: String): LocalDate {
    return try {
        LocalDate.parse(value) // expects YYYY-MM-DD
    } catch (_: DateTimeParseException) {
        throw CliArgException("$flag expects date format YYYY-MM-DD, got: $value")
    }
}
private fun printUsage() {
    println(
        """
        Usage:
          grocery-cli <command> [flags]

        Commands:
          sync                 Pull all departments, all items (paged), then item details
          help                 Show help

        Flags:
          --deptConcurrency N      Parallel departments (default: 2)
          --detailConcurrency N    Parallel item detail calls (default: 10)
          --resume                Resume from checkpoints / skip already-synced items
          --since YYYY-MM-DD       Only pull items updated since date (optional)
          --dryRun                Do not write to storage (just print / simulate)
          --baseUrl URL            API base URL (optional)
          --token TOKEN            API token (optional)
          --out PATH               Output path (optional)
          -h, --help               Show help

        Examples:
          grocery-cli sync --deptConcurrency 3 --detailConcurrency 12
          grocery-cli sync --resume --since 2026-01-01
          grocery-cli sync --dryRun --out ./dump.jsonl
        """.trimIndent()
    )
}
