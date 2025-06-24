# frozen_string_literal: true

require "active_record"

def wait_for_db(host, port)
  require "socket"
  30.times do
    TCPSocket.new(host, port).close
    return
  rescue Errno::ECONNREFUSED, Errno::EHOSTUNREACH
    sleep 1
  end
  raise "DB at #{host}:#{port} did not become available"
end

def setup_docker_databases
  # Postgres
  unless ENV["SKIP_POSTGRES"]
    system("docker run --rm -d --name batchagg_pg -e POSTGRES_PASSWORD=pass -p 54321:5432 postgres:15") unless system("docker ps | grep batchagg_pg")
    wait_for_db("127.0.0.1", 54_321)
    begin
      system("docker exec batchagg_pg psql -U postgres -c 'CREATE DATABASE batchagg_test;'")
    rescue StandardError
      nil
    end
  end

  # MySQL
  return if ENV["SKIP_MYSQL"]

  system("docker run --rm -d --name batchagg_mysql -e MYSQL_ROOT_PASSWORD=pass -e MYSQL_DATABASE=batchagg_test -p 33061:3306 mysql:8") unless system("docker ps | grep batchagg_mysql")
  wait_for_db("127.0.0.1", 33_061)
end

setup_docker_databases unless ENV["SKIP_DOCKER"]

DATABASES = [
  {
    name: "SQLite",
    config: { adapter: "sqlite3", database: ":memory:" }
  },
  {
    name: "PostgreSQL",
    config: {
      adapter: "postgresql",
      host: "127.0.0.1",
      port: 54_321,
      username: "postgres",
      password: "pass",
      database: "batchagg_test"
    }
  },
  {
    name: "MySQL",
    config: {
      adapter: "mysql2",
      host: "127.0.0.1",
      port: 33_061,
      username: "root",
      password: "pass",
      database: "batchagg_test"
    }
  }
].freeze
