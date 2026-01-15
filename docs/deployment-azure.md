# FlyMQ Azure Deployment Guide

This guide covers deploying FlyMQ on Microsoft Azure using Virtual Machines, Azure Container Instances, and Azure Kubernetes Service.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Azure VM Deployment](#azure-vm-deployment)
- [Azure Container Instances](#azure-container-instances)
- [AKS Deployment](#aks-deployment)
- [Network Configuration](#network-configuration)
- [Storage Configuration](#storage-configuration)
- [Monitoring with Azure Monitor](#monitoring-with-azure-monitor)
- [Cost Optimization](#cost-optimization)
- [Terraform Examples](#terraform-examples)

---

## Overview

| Deployment Option | Best For | Complexity | Cost |
|-------------------|----------|------------|------|
| **Azure VMs** | Full control, custom configurations | Medium | Variable |
| **ACI** | Serverless containers, quick deployments | Low | Pay-per-use |
| **AKS** | Kubernetes-native, enterprise scale | High | Higher base cost |

---

## Prerequisites

- Azure subscription with appropriate permissions
- Azure CLI installed (`az login`)
- Terraform 1.5+ (for IaC examples)
- Docker (for building images)

### Required Azure Roles

- Contributor on the resource group
- Network Contributor (for VNet configuration)
- AcrPush (for container registry)

---

## Azure VM Deployment

### VM Size Recommendations

| Workload | VM Size | vCPUs | Memory | Storage |
|----------|---------|-------|--------|---------|
| Development | Standard_B2s | 2 | 4 GB | 64 GB Premium SSD |
| Production (small) | Standard_D2s_v5 | 2 | 8 GB | 256 GB Premium SSD |
| Production (medium) | Standard_D4s_v5 | 4 | 16 GB | 512 GB Premium SSD |
| Production (large) | Standard_D8s_v5 | 8 | 32 GB | 1 TB Premium SSD |

### Quick Start with Cloud-Init

```bash
# Create resource group
az group create --name flymq-rg --location eastus

# Create VM with cloud-init
az vm create \
  --resource-group flymq-rg \
  --name flymq-node-1 \
  --image Ubuntu2204 \
  --size Standard_D2s_v5 \
  --admin-username azureuser \
  --generate-ssh-keys \
  --custom-data cloud-init.yaml \
  --os-disk-size-gb 256 \
  --storage-sku Premium_LRS
```

**cloud-init.yaml:**

```yaml
#cloud-config
package_update: true
packages:
  - docker.io
  - git

runcmd:
  - systemctl start docker
  - systemctl enable docker
  - usermod -aG docker azureuser
  - cd /opt && git clone https://github.com/firefly-oss/flymq
  - cd /opt/flymq && ./deploy/docker/quickstart.sh bootstrap
```

### Network Security Group

```bash
# Create NSG
az network nsg create \
  --resource-group flymq-rg \
  --name flymq-nsg

# Client connections
az network nsg rule create \
  --resource-group flymq-rg \
  --nsg-name flymq-nsg \
  --name AllowClient \
  --priority 100 \
  --destination-port-ranges 9092 \
  --access Allow

# Cluster communication (internal)
az network nsg rule create \
  --resource-group flymq-rg \
  --nsg-name flymq-nsg \
  --name AllowCluster \
  --priority 110 \
  --source-address-prefixes VirtualNetwork \
  --destination-port-ranges 9093 \
  --access Allow

# Health and Admin
az network nsg rule create \
  --resource-group flymq-rg \
  --nsg-name flymq-nsg \
  --name AllowManagement \
  --priority 120 \
  --source-address-prefixes 10.0.0.0/8 \
  --destination-port-ranges 9095-9096 \
  --access Allow
```

---

## Azure Container Instances

### Push Image to ACR

```bash
# Create container registry
az acr create \
  --resource-group flymq-rg \
  --name flymqregistry \
  --sku Basic

# Login to ACR
az acr login --name flymqregistry

# Build and push
docker build -t flymq:latest -f deploy/docker/Dockerfile .
docker tag flymq:latest flymqregistry.azurecr.io/flymq:latest


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
        image: flymqregistry.azurecr.io/flymq:latest
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
      storageClassName: managed-premium
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
    service.beta.kubernetes.io/azure-load-balancer-internal: "false"
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

### Virtual Network Setup

```bash
# Create VNet
az network vnet create \
  --resource-group flymq-rg \
  --name flymq-vnet \
  --address-prefix 10.0.0.0/16 \
  --subnet-name flymq-subnet \
  --subnet-prefix 10.0.1.0/24
```

### Azure Load Balancer

```bash
# Create public IP
az network public-ip create \
  --resource-group flymq-rg \
  --name flymq-pip \
  --sku Standard \
  --allocation-method Static

# Create load balancer
az network lb create \
  --resource-group flymq-rg \
  --name flymq-lb \
  --sku Standard \
  --public-ip-address flymq-pip \
  --frontend-ip-name flymq-frontend \
  --backend-pool-name flymq-backend

# Create health probe
az network lb probe create \
  --resource-group flymq-rg \
  --lb-name flymq-lb \
  --name flymq-health \
  --protocol Http \
  --port 9095 \
  --path /health

# Create load balancing rule
az network lb rule create \
  --resource-group flymq-rg \
  --lb-name flymq-lb \
  --name flymq-rule \
  --protocol Tcp \
  --frontend-port 9092 \
  --backend-port 9092 \
  --frontend-ip-name flymq-frontend \
  --backend-pool-name flymq-backend \
  --probe-name flymq-health
```

---

## Storage Configuration

### Azure Disk Types

| Type | Use Case | IOPS | Throughput |
|------|----------|------|------------|
| **Premium SSD** | Production workloads | Up to 20,000 | 900 MB/s |
| **Premium SSD v2** | High-performance | Up to 80,000 | 1,200 MB/s |
| **Ultra Disk** | Mission-critical | Up to 160,000 | 2,000 MB/s |

### Create Managed Disk

```bash
az disk create \
  --resource-group flymq-rg \
  --name flymq-data-disk \
  --size-gb 512 \
  --sku Premium_LRS \
  --tier P40
```

---

## Monitoring with Azure Monitor

### Enable Container Insights (AKS)

```bash
az aks enable-addons \
  --resource-group flymq-rg \
  --name flymq-aks \
  --addons monitoring \
  --workspace-resource-id /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.OperationalInsights/workspaces/<workspace>
```

### Log Analytics Queries

```kusto
// FlyMQ container logs
ContainerLog
| where LogEntry contains "flymq"
| project TimeGenerated, LogEntry
| order by TimeGenerated desc

// Pod restarts
KubePodInventory
| where Name contains "flymq"
| summarize RestartCount = sum(PodRestartCount) by Name, bin(TimeGenerated, 1h)
```

### Azure Monitor Alerts

```bash
# Create action group
az monitor action-group create \
  --resource-group flymq-rg \
  --name flymq-alerts \
  --short-name flymq \
  --email-receiver name=admin email=admin@example.com

# Create alert rule
az monitor metrics alert create \
  --resource-group flymq-rg \
  --name flymq-high-cpu \
  --scopes /subscriptions/<sub-id>/resourceGroups/flymq-rg/providers/Microsoft.Compute/virtualMachines/flymq-node-1 \
  --condition "avg Percentage CPU > 80" \
  --action flymq-alerts
```

---

## Cost Optimization

### Reserved Instances

| Term | Savings |
|------|---------|
| 1-year | ~30-40% |
| 3-year | ~50-60% |

### Spot VMs for Development

```bash
az vm create \
  --resource-group flymq-rg \
  --name flymq-spot \
  --image Ubuntu2204 \
  --size Standard_D2s_v5 \
  --priority Spot \
  --max-price 0.05 \
  --eviction-policy Deallocate
```

### Azure Advisor Recommendations

```bash
az advisor recommendation list \
  --resource-group flymq-rg \
  --category Cost
```

---

## Terraform Examples

### Complete Azure Deployment

```hcl
# main.tf
provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "flymq" {
  name     = "flymq-rg"
  location = "East US"
}

resource "azurerm_virtual_network" "flymq" {
  name                = "flymq-vnet"
  address_space       = ["10.0.0.0/16"]
  location            = azurerm_resource_group.flymq.location
  resource_group_name = azurerm_resource_group.flymq.name
}

resource "azurerm_subnet" "flymq" {
  name                 = "flymq-subnet"
  resource_group_name  = azurerm_resource_group.flymq.name
  virtual_network_name = azurerm_virtual_network.flymq.name
  address_prefixes     = ["10.0.1.0/24"]
}

resource "azurerm_network_security_group" "flymq" {
  name                = "flymq-nsg"
  location            = azurerm_resource_group.flymq.location
  resource_group_name = azurerm_resource_group.flymq.name

  security_rule {
    name                       = "AllowClient"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "9092"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }

  security_rule {
    name                       = "AllowCluster"
    priority                   = 110
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "9093"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "*"
  }
}

resource "azurerm_linux_virtual_machine" "flymq" {
  count = 3

  name                = "flymq-node-${count.index + 1}"
  resource_group_name = azurerm_resource_group.flymq.name
  location            = azurerm_resource_group.flymq.location
  size                = "Standard_D2s_v5"
  admin_username      = "azureuser"

  network_interface_ids = [azurerm_network_interface.flymq[count.index].id]

  admin_ssh_key {
    username   = "azureuser"
    public_key = file("~/.ssh/id_rsa.pub")
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Premium_LRS"
    disk_size_gb         = 256
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts"
    version   = "latest"
  }

  custom_data = base64encode(file("cloud-init.yaml"))
}

resource "azurerm_network_interface" "flymq" {
  count = 3

  name                = "flymq-nic-${count.index + 1}"
  location            = azurerm_resource_group.flymq.location
  resource_group_name = azurerm_resource_group.flymq.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.flymq.id
    private_ip_address_allocation = "Dynamic"
  }
}

output "private_ips" {
  value = azurerm_network_interface.flymq[*].private_ip_address
}
```

---

**Copyright © 2026 Firefly Software Solutions Inc.**
