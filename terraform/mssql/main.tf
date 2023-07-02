terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "3.0.2"
    }
  }
}

provider "docker" {
  host = "unix:///var/run/docker.sock"
}

resource "docker_container" "azuresqledge" {
  name  = "azuresqledge"
  image = "mcr.microsoft.com/azure-sql-edge"
  ports {
    internal = 1433
    external = 1433
  }
  env = [
    "ACCEPT_EULA=1",
    "MSSQL_SA_PASSWORD=Somang123"
  ]
  volumes {
    host_path      = "/Users/hope/Downloads/StackOverflow2010"
    container_path = "/somang"
  }
}