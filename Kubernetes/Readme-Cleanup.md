# Kubernetes Cleanup Guide

A structured guide to safely clean and reset your Kubernetes environment based on different operational needs.

---

# Overview

During Kubernetes development and testing, cleanup operations are often required for:

* restarting deployments
* resolving configuration issues
* removing unused resources
* preparing environments for fresh installations

This guide provides three levels of cleanup ranging from safe project cleanup to complete cluster reset.

---

# Cleanup Levels

| Level        | Cleanup Type             | Impact                                       |
| ------------ | ------------------------ | -------------------------------------------- |
| Safe Cleanup | Project Resource Cleanup | Removes only project-specific resources      |
| Helm Cleanup | Application Removal      | Removes Helm-managed applications completely |
| Full Reset   | Cluster Reset            | Removes the entire Kubernetes cluster        |

---

# Option 1 — Safe Cleanup (Recommended)

Delete only project-related namespaces.

```bash
kubectl delete namespace airflow
kubectl delete namespace monitoring
kubectl delete namespace rtf-data-pipline
```

## Resources Removed

* Airflow resources
* Monitoring stack resources
* Data pipeline resources

## Resources Preserved

* Kubernetes cluster
* Core system services
* Minikube configuration

## Recommended Usage

Best suited for routine development cleanup and redeployment activities.

---

# Option 2 — Helm Cleanup (Complete Application Removal)

First uninstall Helm releases:

```bash
helm uninstall airflow -n airflow
helm uninstall monitoring -n monitoring
```

Then remove the namespaces:

```bash
kubectl delete namespace airflow
kubectl delete namespace monitoring
kubectl delete namespace rtf-data-pipline
```

## Resources Removed

* Helm-managed applications
* Associated Kubernetes resources
* Application configurations

## Recommended Usage

Use this approach before reinstalling applications or when troubleshooting deployment issues.

---

# Option 3 — Full Cluster Reset

Delete the entire Kubernetes cluster:

```bash
minikube delete
```

---

# Recreate the Kubernetes Cluster

```bash
minikube start --driver=docker --memory=8144 --cpus=4
```

---

# Important Warning

A full cluster reset removes:

* All Pods
* All Deployments
* All Services
* All ConfigMaps
* All Secrets
* All Persistent Volumes
* Entire Minikube cluster configuration

## Recommended Usage

Use only when the cluster environment is unstable or unrecoverable.

---

# Verify Existing Namespaces

Before deleting resources, verify current namespaces:

```bash
kubectl get ns
```

---

# Recommended Cleanup Command

For most development scenarios:

```bash
kubectl delete namespace airflow monitoring rtf-data-pipline
```

## Benefits

* Fast cleanup process
* Safe for development environments
* Removes project-specific resources efficiently

---

# Best Practices

* Use Safe Cleanup during regular development cycles
* Use Helm Cleanup before application reinstallation
* Use Full Reset only for severe cluster issues

---

# Quick Reference

| Objective                | Command                        |
| ------------------------ | ------------------------------ |
| Remove project resources | `kubectl delete namespace ...` |
| Remove Helm applications | `helm uninstall ...`           |
| Reset Kubernetes cluster | `minikube delete`              |

---

# Final Note

Proper Kubernetes cleanup and resource management are essential for maintaining stable and efficient development environments.

Effective cleanup practices help:

* reduce resource conflicts
* improve deployment consistency
* maintain cluster stability
* streamline development workflows

Consistent environment management is an important skill for modern data and platform engineering workflows.
