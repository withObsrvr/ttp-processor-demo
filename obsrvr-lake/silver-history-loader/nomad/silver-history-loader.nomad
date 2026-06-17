job "silver-history-loader" {
  datacenters = ["dc1"]
  type        = "batch"

  parameterized {
    payload       = "forbidden"
    meta_required = [
      "network",
      "start_ledger",
      "end_ledger",
      "chunk_size",
      "bronze_ducklake_catalog",
      "bronze_data_path",
      "silver_ducklake_catalog",
      "silver_data_path"
    ]
  }

  group "loader" {
    task "silver-history-loader" {
      driver = "docker"

      config {
        image = "withobsrvr/silver-history-loader:latest"
        args = [
          "--network", "${NOMAD_META_network}",
          "--start-ledger", "${NOMAD_META_start_ledger}",
          "--end-ledger", "${NOMAD_META_end_ledger}",
          "--chunk-size", "${NOMAD_META_chunk_size}",
          "--bronze-ducklake-catalog", "${NOMAD_META_bronze_ducklake_catalog}",
          "--bronze-data-path", "${NOMAD_META_bronze_data_path}",
          "--silver-ducklake-catalog", "${NOMAD_META_silver_ducklake_catalog}",
          "--silver-data-path", "${NOMAD_META_silver_data_path}",
          "--resume"
        ]
      }

      env {
        # Inject from Vault/Nomad template in production; do not hard-code secrets here.
        S3_KEY_ID = ""
        S3_SECRET = ""
      }

      resources {
        cpu    = 4000
        memory = 8192
      }
    }
  }
}
