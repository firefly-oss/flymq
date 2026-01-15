# FlyMQ AWS Deployment Guide

This guide covers deploying FlyMQ on Amazon Web Services using EC2, ECS/Fargate, and EKS.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [EC2 Deployment](#ec2-deployment)
- [ECS/Fargate Deployment](#ecsfargate-deployment)
- [EKS Deployment](#eks-deployment)
- [Network Configuration](#network-configuration)
- [Storage Configuration](#storage-configuration)
- [Monitoring with CloudWatch](#monitoring-with-cloudwatch)
- [Cost Optimization](#cost-optimization)
- [Terraform Examples](#terraform-examples)

---

## Overview

| Deployment Option | Best For | Complexity | Cost |
|-------------------|----------|------------|------|
| **EC2** | Full control, custom configurations | Medium | Variable |
| **ECS/Fargate** | Serverless containers, auto-scaling | Low | Pay-per-use |
| **EKS** | Kubernetes-native, multi-cloud | High | Higher base cost |

---

## Prerequisites

- AWS Account with appropriate permissions
- AWS CLI configured (`aws configure`)
- Terraform 1.5+ (for IaC examples)
- Docker (for building images)

### Required IAM Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ec2:*",
        "ecs:*",
        "eks:*",
        "ecr:*",
        "elasticloadbalancing:*",
        "cloudwatch:*",
        "logs:*",
        "iam:PassRole"
      ],
      "Resource": "*"
    }
  ]
}
```

---

## EC2 Deployment

### Instance Recommendations

| Workload | Instance Type | vCPUs | Memory | Storage |
|----------|---------------|-------|--------|---------|
| Development | t3.medium | 2 | 4 GB | 50 GB gp3 |
| Production (small) | m6i.large | 2 | 8 GB | 200 GB gp3 |
| Production (medium) | m6i.xlarge | 4 | 16 GB | 500 GB gp3 |
| Production (large) | m6i.2xlarge | 8 | 32 GB | 1 TB io2 |

### Quick Start with User Data

Launch an EC2 instance with this user data script:

```bash
#!/bin/bash
set -e

# Install Docker
yum update -y
yum install -y docker
systemctl start docker
systemctl enable docker

# Install FlyMQ
cd /opt
git clone https://github.com/firefly-oss/flymq
cd flymq

# Start as bootstrap server
./deploy/docker/quickstart.sh bootstrap
```

### Manual EC2 Setup

1. **Launch EC2 Instance:**

```bash
aws ec2 run-instances \
  --image-id ami-0c55b159cbfafe1f0 \
  --instance-type m6i.large \
  --key-name your-key \
  --security-group-ids sg-xxxxxxxx \
  --subnet-id subnet-xxxxxxxx \
  --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":200,"VolumeType":"gp3"}}]' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=flymq-node-1}]'
```

2. **Connect and Install:**

```bash
ssh -i your-key.pem ec2-user@<instance-ip>

# Install FlyMQ
sudo yum install -y git docker
sudo systemctl start docker
git clone https://github.com/firefly-oss/flymq
cd flymq
./deploy/docker/quickstart.sh bootstrap
```

3. **Add Additional Nodes:**

On each additional EC2 instance, run the join command shown in the bootstrap output.

### Security Group Configuration

```bash
# Create security group
aws ec2 create-security-group \
  --group-name flymq-sg \
  --description "FlyMQ Security Group" \
  --vpc-id vpc-xxxxxxxx

# Client connections (from anywhere or specific CIDR)
aws ec2 authorize-security-group-ingress \
  --group-id sg-xxxxxxxx \
  --protocol tcp --port 9092 --cidr 0.0.0.0/0

# Cluster communication (internal only)
aws ec2 authorize-security-group-ingress \
  --group-id sg-xxxxxxxx \
  --protocol tcp --port 9093 --source-group sg-xxxxxxxx

# Health checks
aws ec2 authorize-security-group-ingress \
  --group-id sg-xxxxxxxx \


### Create ECS Service

```bash
# Create ECS cluster
aws ecs create-cluster --cluster-name flymq-cluster

# Register task definition
aws ecs register-task-definition --cli-input-json file://task-definition.json

# Create service
aws ecs create-service \
  --cluster flymq-cluster \
  --service-name flymq-service \
  --task-definition flymq \
  --desired-count 3 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-xxx,subnet-yyy],securityGroups=[sg-xxx],assignPublicIp=ENABLED}"
```

---

## EKS Deployment

### Create EKS Cluster

```bash
# Create cluster with eksctl
eksctl create cluster \
  --name flymq-cluster \
  --region us-east-1 \
  --nodegroup-name flymq-nodes \
  --node-type m6i.large \
  --nodes 3 \
  --nodes-min 3 \
  --nodes-max 6
```

### Kubernetes Manifests

**Namespace and ConfigMap:**

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: flymq
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: flymq-config
  namespace: flymq
data:
  FLYMQ_LOG_LEVEL: "info"
  FLYMQ_BIND_ADDR: "0.0.0.0:9092"
  FLYMQ_CLUSTER_ADDR: "0.0.0.0:9093"
  FLYMQ_HEALTH_ENABLED: "true"
  FLYMQ_ADMIN_ENABLED: "true"
```

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
        image: <account-id>.dkr.ecr.us-east-1.amazonaws.com/flymq:latest
        ports:
        - containerPort: 9092
          name: client
        - containerPort: 9093
          name: cluster
        - containerPort: 9095
          name: health
        - containerPort: 9096
          name: admin
        envFrom:
        - configMapRef:
            name: flymq-config
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
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health/ready
            port: 9095
          initialDelaySeconds: 10
          periodSeconds: 5
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: gp3
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
aws ec2 create-vpc --cidr-block 10.0.0.0/16

# Create subnets in multiple AZs
aws ec2 create-subnet --vpc-id vpc-xxx --cidr-block 10.0.1.0/24 --availability-zone us-east-1a
aws ec2 create-subnet --vpc-id vpc-xxx --cidr-block 10.0.2.0/24 --availability-zone us-east-1b
aws ec2 create-subnet --vpc-id vpc-xxx --cidr-block 10.0.3.0/24 --availability-zone us-east-1c
```

### Network Load Balancer

```bash
# Create NLB
aws elbv2 create-load-balancer \
  --name flymq-nlb \
  --type network \
  --subnets subnet-xxx subnet-yyy subnet-zzz

# Create target group
aws elbv2 create-target-group \
  --name flymq-targets \
  --protocol TCP \
  --port 9092 \
  --vpc-id vpc-xxx \
  --health-check-protocol HTTP \
  --health-check-path /health \
  --health-check-port 9095

# Create listener
aws elbv2 create-listener \
  --load-balancer-arn arn:aws:elasticloadbalancing:... \
  --protocol TCP \
  --port 9092 \
  --default-actions Type=forward,TargetGroupArn=arn:aws:elasticloadbalancing:...
```

---

## Storage Configuration

### EBS Volume Types

| Type | Use Case | IOPS | Throughput |
|------|----------|------|------------|
| **gp3** | General purpose, cost-effective | 3,000-16,000 | 125-1,000 MB/s |
| **io2** | High-performance, mission-critical | Up to 64,000 | 1,000 MB/s |
| **st1** | Throughput-optimized, cold data | 500 | 500 MB/s |

### EBS Optimization

```bash
# Create optimized gp3 volume
aws ec2 create-volume \
  --availability-zone us-east-1a \
  --size 500 \
  --volume-type gp3 \
  --iops 10000 \
  --throughput 500 \
  --tag-specifications 'ResourceType=volume,Tags=[{Key=Name,Value=flymq-data}]'
```

---

## Monitoring with CloudWatch

### CloudWatch Agent Configuration

```json
{
  "metrics": {
    "namespace": "FlyMQ",
    "metrics_collected": {
      "prometheus": {
        "prometheus_config_path": "/opt/aws/amazon-cloudwatch-agent/etc/prometheus.yml",
        "emf_processor": {
          "metric_namespace": "FlyMQ",
          "metric_unit": {
            "flymq_messages_produced_total": "Count",
            "flymq_messages_consumed_total": "Count"
          }
        }
      }
    }
  }
}
```

### CloudWatch Alarms

```bash
# No leader alarm
aws cloudwatch put-metric-alarm \
  --alarm-name flymq-no-leader \
  --metric-name flymq_cluster_leader \
  --namespace FlyMQ \
  --statistic Sum \
  --period 60 \
  --threshold 0 \
  --comparison-operator LessThanOrEqualToThreshold \
  --evaluation-periods 2 \
  --alarm-actions arn:aws:sns:us-east-1:xxx:alerts

# High consumer lag alarm
aws cloudwatch put-metric-alarm \
  --alarm-name flymq-consumer-lag \
  --metric-name flymq_consumer_group_lag \
  --namespace FlyMQ \
  --statistic Maximum \
  --period 300 \
  --threshold 10000 \
  --comparison-operator GreaterThanThreshold \
  --evaluation-periods 2 \
  --alarm-actions arn:aws:sns:us-east-1:xxx:alerts
```

---

## Cost Optimization

### Reserved Instances

For production workloads running 24/7, use Reserved Instances:

| Term | Savings |
|------|---------|
| 1-year, no upfront | ~30% |
| 1-year, partial upfront | ~40% |
| 3-year, all upfront | ~60% |

### Spot Instances for Development

```bash
aws ec2 request-spot-instances \
  --instance-count 1 \
  --type one-time \
  --launch-specification file://spot-spec.json
```

### Right-Sizing Recommendations

1. Start with smaller instances and scale up based on metrics
2. Use CloudWatch Container Insights for ECS/EKS
3. Enable Compute Optimizer for recommendations

---

## Terraform Examples

### Complete EC2 Cluster

```hcl
# main.tf
provider "aws" {
  region = "us-east-1"
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "5.0.0"

  name = "flymq-vpc"
  cidr = "10.0.0.0/16"

  azs             = ["us-east-1a", "us-east-1b", "us-east-1c"]
  private_subnets = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  public_subnets  = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]

  enable_nat_gateway = true
  single_nat_gateway = true
}

resource "aws_security_group" "flymq" {
  name        = "flymq-sg"
  description = "FlyMQ Security Group"
  vpc_id      = module.vpc.vpc_id

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 9093
    to_port     = 9093
    protocol    = "tcp"
    self        = true
  }

  ingress {
    from_port   = 9095
    to_port     = 9096
    protocol    = "tcp"
    cidr_blocks = [module.vpc.vpc_cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "flymq" {
  count = 3

  ami           = data.aws_ami.amazon_linux.id
  instance_type = "m6i.large"
  subnet_id     = module.vpc.private_subnets[count.index]

  vpc_security_group_ids = [aws_security_group.flymq.id]

  root_block_device {
    volume_size = 200
    volume_type = "gp3"
    iops        = 5000
    throughput  = 250
  }

  user_data = base64encode(templatefile("${path.module}/user-data.sh", {
    node_index = count.index
    cluster_token = random_password.cluster_token.result
  }))

  tags = {
    Name = "flymq-node-${count.index + 1}"
  }
}

resource "random_password" "cluster_token" {
  length  = 32
  special = false
}

data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-x86_64-gp2"]
  }
}

output "instance_ips" {
  value = aws_instance.flymq[*].private_ip
}
```

---

**Copyright © 2026 Firefly Software Solutions Inc.**
