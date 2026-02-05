package com.ericho.dropit.model.adapter

import com.ericho.dropit.model.DatabaseConfig
import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.api.SnapshotPayload
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.jooq.JSONB
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.sql.Timestamp
import java.time.Instant

class PostgresqlStorage(private val config: DatabaseConfig) : Storage {
    private val json = Json { encodeDefaults = true }
    private val dataSource: HikariDataSource = HikariDataSource(
        HikariConfig().apply {
            jdbcUrl = "jdbc:postgresql://${config.host}:${config.port}/${config.database}"
            driverClassName = "org.postgresql.Driver"
            username = config.user
            password = config.password
            poolName = "dropit-postgres-storage-pool"
            minimumIdle = 1
            maximumPoolSize = 8
            connectionTestQuery = "SELECT 1"
            initializationFailTimeout = 10_000
        }
    )

    init {
        try {
            dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.POSTGRES)
                ensureSnapshotsSchema(ctx)
            }
        } catch (exception: Exception) {
            fail(
                operation = "initSchema",
                key = "n/a",
                exception = exception
            )
        }
    }

    override fun upsertSnapshot(detail: SingleProductPayload) {
        val snapshot = SnapshotPayload(
            key = detail.id,
            json = json.encodeToString(detail)
        )
        writeSnapshot(snapshot)
    }

    override fun close() {
        dataSource.close()
    }

    private fun writeSnapshot(snapshot: SnapshotPayload) {
        val now = Timestamp.from(Instant.now())
        try {
            dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.POSTGRES)
                val table = DSL.table("product_snapshots")
                val snapshotKey = DSL.field("snapshot_key", String::class.java)
                val payload = DSL.field("payload", SQLDataType.JSONB)
                val createdAt = DSL.field("created_at", Timestamp::class.java)

                ctx.insertInto(table)
                    .columns(snapshotKey, payload, createdAt)
                    .values(snapshot.key, JSONB.jsonb(snapshot.json), now)
                    .onConflict(snapshotKey)
                    .doUpdate()
                    .set(payload, JSONB.jsonb(snapshot.json))
                    .set(createdAt, now)
                    .execute()
            }
        } catch (exception: Exception) {
            fail(
                operation = "upsertSnapshot",
                key = snapshot.key,
                exception = exception
            )
        }
    }

    private fun ensureSnapshotsSchema(ctx: org.jooq.DSLContext) {
        val table = DSL.table("product_snapshots")
        val id = DSL.field("id", SQLDataType.BIGINT.identity(true).nullable(false))
        val snapshotKey = DSL.field("snapshot_key", SQLDataType.VARCHAR(255).nullable(false))
        val payload = DSL.field("payload", SQLDataType.JSONB.nullable(false))
        val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))

        ctx.createTableIfNotExists(table)
            .column(id)
            .column(snapshotKey)
            .column(payload)
            .column(createdAt)
            .constraints(DSL.primaryKey(id))
            .execute()

        ctx.createUniqueIndexIfNotExists("ux_product_snapshots_snapshot_key")
            .on(table, snapshotKey)
            .execute()
    }

    private fun fail(operation: String, key: String, exception: Exception): Nothing {
        System.err.println(
            "event=storage_error backend=postgres operation=$operation key=$key " +
                "error=${exception::class.simpleName} message=${exception.message}"
        )
        throw StorageWriteException(
            backend = "postgres",
            operation = operation,
            key = key,
            cause = exception
        )
    }
}
