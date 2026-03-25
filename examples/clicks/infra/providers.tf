terraform {
  required_providers {
    kafka = {
      source  = "Mongey/kafka"
      version = "~> 0.7"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
  }
}

provider "kafka" {
  alias            = "prod"
  bootstrap_servers = ["redpanda-prod:9092"]
}

provider "kubernetes" {
}
