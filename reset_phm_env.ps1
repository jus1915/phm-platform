<# 
    reset_phm_env.ps1

    - docker compose down -v
    - MinIO 데이터 삭제 (./minio-data)
    - Postgres 데이터 삭제 (./pgdata)
    - (선택) MLflow 데이터 삭제 (./mlruns)
    - docker compose up -d --build
    - MinIO에 phm-raw 버킷 생성
#>

Write-Host "=== [1] Docker Compose Down (with volumes) ===" -ForegroundColor Cyan
docker compose down -v

Write-Host "`n=== [2] Delete MinIO + Postgres + MLflow Data ===" -ForegroundColor Cyan

# MinIO 데이터 폴더 삭제
if (Test-Path ".\minio-data") {
    Write-Host "Deleting .\minio-data ..." -ForegroundColor Yellow
    Remove-Item -Recurse -Force ".\minio-data"
    Write-Host "Deleted: .\minio-data" -ForegroundColor Green
} else {
    Write-Host "minio-data not found. Skip." -ForegroundColor DarkGray
}

# Postgres 데이터 폴더 삭제
if (Test-Path ".\pgdata") {
    Write-Host "Deleting .\pgdata ..." -ForegroundColor Yellow
    Remove-Item -Recurse -Force ".\pgdata"
    Write-Host "Deleted: .\pgdata" -ForegroundColor Green
} else {
    Write-Host "pgdata not found. Skip." -ForegroundColor DarkGray
}

# MLflow run 데이터 폴더 삭제 (있으면)
if (Test-Path ".\mlruns") {
    Write-Host "Deleting .\mlruns (MLflow runs) ..." -ForegroundColor Yellow
    Remove-Item -Recurse -Force ".\mlruns"
    Write-Host "Deleted: .\mlruns" -ForegroundColor Green
} else {
    Write-Host "mlruns not found. Skip." -ForegroundColor DarkGray
}

Write-Host "`n=== [3] Docker Compose Up (rebuild) ===" -ForegroundColor Cyan
docker compose up -d --build

Write-Host "`n=== [4] Wait for MinIO to initialize ===" -ForegroundColor Cyan
Start-Sleep -Seconds 8

Write-Host "`n=== [5] Create MinIO bucket: phm-raw ===" -ForegroundColor Cyan

# MinIO 컨테이너 이름이 docker-compose.yml에서 'minio' 로 정의되어 있다고 가정
# MinIO 계정: admin / admin12345 (DAG 에서 사용하던 값과 맞춤)
$minioCmd = @"
mc alias set local http://localhost:9000 admin admin12345 && \
mc mb -p local/phm-raw || echo 'bucket may already exist'
"@

docker exec minio sh -c "$minioCmd"

Write-Host "`n=== ✅ Done ===" -ForegroundColor Green
Write-Host "MinIO + Postgres + MLflow 리셋 완료, 'phm-raw' 버킷 생성됨." -ForegroundColor Green
Write-Host "이제 DAQ에서 새 세션 수집 → DAG(bronze_to_silver_and_features) 돌려서 raw/mart/MLflow를 다시 채워보면 됨." -ForegroundColor Cyan
