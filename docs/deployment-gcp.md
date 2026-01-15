# FlyMQ Google Cloud Deployment Guide

This guide covers deploying FlyMQ on Google Cloud Platform using Compute Engine, Cloud Run, and Google Kubernetes Engine.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Compute Engine Deployment](#compute-engine-deployment)
- [Cloud Run Deployment](#cloud-run-deployment)
- [GKE Deployment](#gke-deployment)
- [Network Configuration](#network-configuration)
- [Storage Configuration](#storage-configuration)
- [Monitoring with Cloud Monitoring](#monitoring-with-cloud-monitoring)
- [Cost Optimization](#cost-optimization)
- [Terraform Examples](#terraform-examples)

---

## Overview

| Deployment Option | Best For | Complexity | Cost |
|-------------------|----------|------------|------|
| **Compute Engine** | Full control, custom configurations | Medium | Variable |
| **Cloud Run** | Serverless, auto-scaling | Low | Pay-per-use |
| **GKE** | Kubernetes-native, enterprise scale | High | Higher base cost |

---

## Prerequisites

- Google Cloud project with billing enabled
- gcloud CLI installed and configured (`gcloud auth login`)
- Terraform 1.5+ (for IaC examples)
- Docker (for building images)

### Required IAM Roles

- `roles/compute.admin`
- `roles/container.admin`
- `roles/run.admin`
- `roles/artifactregistry.admin`

---

## Compute Engine Deployment

### Machine Type Recommendations

| Workload | Machine Type | vCPUs | Memory | Storage |
|----------|--------------|-------|--------|---------|
| Development | e2-medium | 2 | 4 GB | 50 GB SSD |
| Production (small) | n2-standard-2 | 2 | 8 GB | 200 GB SSD |
| Production (medium) | n2-standard-4 | 4 | 16 GB | 500 GB SSD |
| Production (large) | n2-standard-8 | 8 | 32 GB | 1 TB SSD |

### Quick Start with Startup Script

```bash
# Create instance with startup script
gcloud compute instances create flymq-node-1 \
  --zone=us-central1-a \
  --machine-type=n2-standard-2 \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=200GB \
  --boot-disk-type=pd-ssd \
  --tags=flymq \
  --metadata-from-file=startup-script=startup.sh
```

**startup.sh:**

```bash
#!/bin/bash
set -e

# Install Docker
apt-get update
apt-get install -y docker.io git
systemctl start docker
systemctl enable docker

# Install FlyMQ
cd /opt
git clone https://github.com/firefly-oss/flymq
cd flymq

# Start as bootstrap server
./deploy/docker/quickstart.sh bootstrap
```

### Firewall Rules

```bash
# Create firewall rules
gcloud compute firewall-rules create flymq-client \
  --allow=tcp:9092 \
  --target-tags=flymq \
  --description="FlyMQ client connections"

gcloud compute firewall-rules create flymq-cluster \
  --allow=tcp:9093 \
  --source-tags=flymq \
  --target-tags=flymq \
  --description="FlyMQ cluster communication"

gcloud compute firewall-rules create flymq-management \
  --allow=tcp:9095-9096 \
  --source-ranges=10.0.0.0/8 \
  --target-tags=flymq \
  --description="FlyMQ health and admin"
```

### Instance Group for High Availability

```bash
# Create instance template
gcloud compute instance-templates create flymq-template \
  --machine-type=n2-standard-2 \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=200GB \
  --boot-disk-type=pd-ssd \
  --tags=flymq \
  --metadata-from-file=startup-script=startup.sh

# Create managed instance group
gcloud compute instance-groups managed create flymq-mig \
  --template=flymq-template \
  --size=3 \
  --zone=us-central1-a
```

---

## Cloud Run Deployment

### Push Image to Artifact Registry

```bash
# Create repository
gcloud artifacts repositories create flymq \
  --repository-format=docker \
  --location=us-central1

# Configure Docker
gcloud auth configure-docker us-central1-docker.pkg.dev



### Kubernetes Manifests

**StatefulSet:**

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: flymq
  namespace: flymq
spec:
  serviceName: flymq-headless
  replicas: 3
  selector:
    matchLabels:
      app: flymq
  template:
    metadata:
      labels:
        app: flymq
    spec:
      containers:
      - name: flymq
        image: us-central1-docker.pkg.dev/PROJECT_ID/flymq/flymq:latest
        ports:
        - containerPort: 9092
          name: client
        - containerPort: 9093
          name: cluster
        - containerPort: 9095
          name: health
        - containerPort: 9096
          name: admin
        env:
        - name: FLYMQ_NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: FLYMQ_CLUSTER_PEERS
          value: "flymq-0.flymq-headless:9093,flymq-1.flymq-headless:9093,flymq-2.flymq-headless:9093"
        volumeMounts:
        - name: data
          mountPath: /data
        livenessProbe:
          httpGet:
            path: /health/live
            port: 9095
          initialDelaySeconds: 30
        readinessProbe:
          httpGet:
            path: /health/ready
            port: 9095
          initialDelaySeconds: 10
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: premium-rwo
      resources:
        requests:
          storage: 100Gi
```

**Services:**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: flymq-headless
  namespace: flymq
spec:
  clusterIP: None
  selector:
    app: flymq
  ports:
  - port: 9093
    name: cluster
---
apiVersion: v1
kind: Service
metadata:
  name: flymq
  namespace: flymq
  annotations:
    cloud.google.com/neg: '{"ingress": true}'
spec:
  type: LoadBalancer
  selector:
    app: flymq
  ports:
  - port: 9092
    name: client
  - port: 9095
    name: health
  - port: 9096
    name: admin
```

---

## Network Configuration

### VPC Setup

```bash
# Create VPC
gcloud compute networks create flymq-vpc \
  --subnet-mode=custom

# Create subnet
gcloud compute networks subnets create flymq-subnet \
  --network=flymq-vpc \
  --region=us-central1 \
  --range=10.0.0.0/24
```

### Cloud Load Balancer

```bash
# Create health check
gcloud compute health-checks create http flymq-health \
  --port=9095 \
  --request-path=/health

# Create backend service
gcloud compute backend-services create flymq-backend \
  --protocol=TCP \
  --health-checks=flymq-health \
  --global

# Create forwarding rule
gcloud compute forwarding-rules create flymq-frontend \
  --global \
  --target-tcp-proxy=flymq-proxy \
  --ports=9092
```

---

## Storage Configuration

### Persistent Disk Types

| Type | Use Case | IOPS | Throughput |
|------|----------|------|------------|
| **pd-ssd** | General SSD | 30,000 | 480 MB/s |
| **pd-extreme** | High-performance | 120,000 | 2,400 MB/s |
| **pd-balanced** | Cost-effective SSD | 15,000 | 240 MB/s |

### Create Persistent Disk

```bash
gcloud compute disks create flymq-data \
  --size=500GB \
  --type=pd-ssd \
  --zone=us-central1-a
```

---

## Monitoring with Cloud Monitoring

### Enable Monitoring

```bash
# Enable APIs
gcloud services enable monitoring.googleapis.com
gcloud services enable logging.googleapis.com
```

### Custom Metrics with Prometheus

```yaml
# prometheus-config.yaml for GKE
apiVersion: v1
kind: ConfigMap
metadata:
  name: prometheus-config
data:
  prometheus.yml: |
    scrape_configs:
      - job_name: 'flymq'
        kubernetes_sd_configs:
          - role: pod
        relabel_configs:
          - source_labels: [__meta_kubernetes_pod_label_app]
            regex: flymq
            action: keep
```

### Cloud Monitoring Alerts

```bash
# Create notification channel
gcloud alpha monitoring channels create \
  --display-name="FlyMQ Alerts" \
  --type=email \
  --channel-labels=email_address=admin@example.com

# Create alert policy
gcloud alpha monitoring policies create \
  --display-name="FlyMQ High CPU" \
  --condition-display-name="CPU > 80%" \
  --condition-filter='metric.type="compute.googleapis.com/instance/cpu/utilization"' \
  --condition-threshold-value=0.8 \
  --condition-threshold-comparison=COMPARISON_GT \
  --notification-channels=CHANNEL_ID
```

---

## Cost Optimization

### Committed Use Discounts

| Term | Savings |
|------|---------|
| 1-year | ~37% |
| 3-year | ~55% |

### Preemptible VMs for Development

```bash
gcloud compute instances create flymq-dev \
  --zone=us-central1-a \
  --machine-type=n2-standard-2 \
  --preemptible \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud
```

### Spot VMs (newer alternative)

```bash
gcloud compute instances create flymq-spot \
  --zone=us-central1-a \
  --machine-type=n2-standard-2 \
  --provisioning-model=SPOT \
  --instance-termination-action=STOP
```

---

## Terraform Examples

### Complete GCP Deployment

```hcl
# main.tf
provider "google" {
  project = var.project_id
  region  = "us-central1"
}

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

resource "google_compute_network" "flymq" {
  name                    = "flymq-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "flymq" {
  name          = "flymq-subnet"
  ip_cidr_range = "10.0.0.0/24"
  region        = "us-central1"
  network       = google_compute_network.flymq.id
}

resource "google_compute_firewall" "flymq_client" {
  name    = "flymq-client"
  network = google_compute_network.flymq.name

  allow {
    protocol = "tcp"
    ports    = ["9092"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["flymq"]
}

resource "google_compute_firewall" "flymq_cluster" {
  name    = "flymq-cluster"
  network = google_compute_network.flymq.name

  allow {
    protocol = "tcp"
    ports    = ["9093"]
  }

  source_tags = ["flymq"]
  target_tags = ["flymq"]
}

resource "google_compute_instance" "flymq" {
  count = 3

  name         = "flymq-node-${count.index + 1}"
  machine_type = "n2-standard-2"
  zone         = "us-central1-a"

  tags = ["flymq"]

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = 200
      type  = "pd-ssd"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.flymq.id
    access_config {}
  }

  metadata_startup_script = file("startup.sh")
}

output "instance_ips" {
  value = google_compute_instance.flymq[*].network_interface[0].network_ip
}
```

---

**Copyright © 2026 Firefly Software Solutions Inc.**

