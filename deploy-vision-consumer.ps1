# deploy-vision-consumer.ps1
# Rebuilds the consumer image, loads it into Minikube, and recreates the deployment + service.

$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

Write-Host "=== Recreating vision-consumer deployment ==="

kubectl delete -f k8s\consumer-deployment.yaml
kubectl delete -f k8s\consumer-service.yaml

docker build -t vision-consumer:latest .\consumer

Write-Host "=== Loading image into Minikube ==="
minikube image load vision-consumer:latest

Write-Host "=== Applying Kubernetes manifests ==="
kubectl apply -f k8s\consumer-deployment.yaml
kubectl apply -f k8s\consumer-service.yaml

Write-Host "=== Checking pod status ==="
$maxAttempts = 20
$attempt = 0

while ($true) {
    $podsOutput = kubectl get pods -l app=vision-consumer --no-headers
    Write-Host $podsOutput

    $isRunning = $podsOutput -match 'Running'
    if ($isRunning) {
        Write-Host "Consumer pod is Running."
        break
    }

    $attempt++
    if ($attempt -ge $maxAttempts) {
        throw "Consumer pod did not reach Running status after $maxAttempts attempts."
    }

    Write-Host "Pod is not Running yet. Waiting 3 seconds before retrying..."
    Start-Sleep -Seconds 3
}

Write-Host "=== Tailing logs from deployment/vision-consumer ==="
kubectl logs -f deployment/vision-consumer
