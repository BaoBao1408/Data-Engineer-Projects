#!/bin/bash
# =============================================================================
# deployment/entrypoint.sh
#
# Smart container entrypoint — handles every runtime command in one script.
#
# Usage inside Docker:
#   docker run --env-file .env adstart-pipeline setup
#   docker run --env-file .env adstart-pipeline run --date 2026-01-15
#   docker run --env-file .env adstart-pipeline upload --date 2026-01-15
#   docker run --env-file .env adstart-pipeline test [unit|integration]
#   docker run --env-file .env adstart-pipeline shell
#
# Usage via docker-compose:
#   docker-compose run --rm setup
#   docker-compose run --rm pipeline run --date 2026-01-15
#   docker-compose run --rm pipeline run --backfill-days 7
#   docker-compose run --rm pipeline upload --date 2026-01-15
#   docker-compose run --rm test
# =============================================================================

set -euo pipefail   # exit immediately on any error, treat unset vars as errors

# -----------------------------------------------------------------------------
# Colour helpers
# -----------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

log_info()   { echo -e "${GREEN}[INFO]${NC}  $*"; }
log_warn()   { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error()  { echo -e "${RED}[ERROR]${NC} $*"; }
log_header() { echo -e "\n${BOLD}${CYAN}$*${NC}"; }
log_step()   { echo -e "  ${CYAN}>${NC} $*"; }

# -----------------------------------------------------------------------------
# Banner
# -----------------------------------------------------------------------------
echo -e "${BOLD}"
echo "  +======================================================+"
echo "  |     adstart Data Pipeline -- AWS Edition             |"
echo "  |     S3 + Glue + Athena + Prefect                     |"
echo "  +======================================================+"
echo -e "${NC}"

# -----------------------------------------------------------------------------
# check_env
#
# Validates that all required environment variables are present and that
# AWS credentials are reachable. Only runs the AWS checks when
# PIPELINE_ENV=aws; local mode skips them entirely.
# -----------------------------------------------------------------------------
check_env() {
    log_header "Checking environment..."

    PIPELINE_ENV="${PIPELINE_ENV:-local}"
    log_step "PIPELINE_ENV = $PIPELINE_ENV"

    if [ "$PIPELINE_ENV" != "aws" ]; then
        log_info "Local mode — skipping AWS environment checks."
        return 0
    fi

    # Required environment variables for AWS mode
    REQUIRED_VARS=(
        "AWS_REGION"
        "AWS_RAW_BUCKET"
        "AWS_WAREHOUSE_BUCKET"
        "AWS_ATHENA_OUTPUT_BUCKET"
        "GLUE_RAW_DATABASE"
        "GLUE_WAREHOUSE_DATABASE"
    )

    MISSING=()
    for var in "${REQUIRED_VARS[@]}"; do
        if [ -z "${!var:-}" ]; then
            MISSING+=("$var")
        else
            log_step "$var = ${!var}"
        fi
    done

    if [ ${#MISSING[@]} -ne 0 ]; then
        log_error "The following required environment variables are not set:"
        for v in "${MISSING[@]}"; do
            echo -e "    ${RED}x${NC} $v"
        done
        echo ""
        echo "  Copy .env.example to .env and fill in your AWS account values."
        echo "  See SETUP_AWS_CONNECTIONS.md for step-by-step instructions."
        exit 1
    fi

    # Verify AWS credentials are valid and reachable
    log_step "Verifying AWS credentials..."
    if python3 -c "
import boto3, sys
try:
    sts = boto3.client('sts', region_name='${AWS_REGION:-eu-west-1}')
    identity = sts.get_caller_identity()
    print(f'  Credentials OK  Account: {identity[\"Account\"]}  ARN: {identity[\"Arn\"].split(\"/\")[-1]}')
except Exception as exc:
    print(f'  Credentials FAILED: {exc}', file=sys.stderr)
    sys.exit(1)
"; then
        log_info "AWS credentials are valid."
    else
        log_error "AWS credentials check failed."
        echo ""
        echo "  Option 1 — explicit keys in .env:"
        echo "    AWS_ACCESS_KEY_ID=AKIA..."
        echo "    AWS_SECRET_ACCESS_KEY=..."
        echo ""
        echo "  Option 2 — mount your local AWS config (dev only):"
        echo "    -v ~/.aws:/root/.aws:ro"
        echo ""
        echo "  Option 3 — attach an IAM role to the EC2 / ECS task (production)."
        exit 1
    fi
}

# -----------------------------------------------------------------------------
# Commands
# -----------------------------------------------------------------------------

cmd_setup() {
    # Create all AWS resources: S3 buckets, Glue databases, IAM role, SNS topic.
    # Run once before the first pipeline execution.
    log_header "Setting up AWS resources..."

    ACCOUNT_ID=$(python3 -c "
import boto3
sts = boto3.client('sts', region_name='${AWS_REGION:-eu-west-1}')
print(sts.get_caller_identity()['Account'])
")
    log_step "Account : $ACCOUNT_ID"
    log_step "Region  : ${AWS_REGION:-eu-west-1}"

    python3 infrastructure/setup_aws.py \
        --account-id "$ACCOUNT_ID" \
        --region "${AWS_REGION:-eu-west-1}" \
        ${ALERT_EMAIL:+--alert-email "$ALERT_EMAIL"}

    log_info "AWS setup complete."
    echo ""
    echo -e "  ${YELLOW}Next steps:${NC}"
    echo "    docker-compose run --rm pipeline upload --date 2026-01-15"
    echo "    docker-compose run --rm pipeline run    --date 2026-01-15"
}

cmd_upload() {
    # Upload local CSV sample data to the S3 raw bucket.
    # Useful for dev/test runs before real operator files arrive.
    log_header "Uploading sample data to S3..."
    python3 infrastructure/upload_sample_data.py "$@"
}

cmd_run() {
    # Execute the main ELT pipeline for one date (or a backfill range).
    #
    # Examples:
    #   cmd_run                          # defaults to yesterday
    #   cmd_run --date 2026-01-15
    #   cmd_run --backfill-days 7
    log_header "Running pipeline..."
    python3 run_pipeline.py "$@"
}

cmd_test() {
    # Run the test suite.
    # Always uses PIPELINE_ENV=local so no real AWS resources are touched.
    log_header "Running tests..."
    case "${1:-all}" in
        unit)        python3 -m pytest tests/unit/        -v ;;
        integration) python3 -m pytest tests/integration/ -v ;;
        *)           python3 -m pytest tests/             -v ;;
    esac
}

cmd_shell() {
    # Drop into an interactive bash shell for debugging.
    # Useful for inspecting the container filesystem or running ad-hoc commands.
    log_header "Starting interactive shell..."
    exec /bin/bash
}

cmd_help() {
    echo -e "${BOLD}Usage:${NC}"
    echo "  docker run --env-file .env adstart-pipeline <command> [options]"
    echo ""
    echo -e "${BOLD}Commands:${NC}"
    echo "  setup                        Create AWS resources (S3, Glue, IAM, SNS)"
    echo "  upload --date DATE           Upload local CSV files to S3 raw bucket"
    echo "  run                          Run pipeline for yesterday (D-1)"
    echo "  run --date DATE              Run pipeline for a specific date"
    echo "  run --backfill-days N        Run pipeline for the last N days"
    echo "  test                         Run the full test suite"
    echo "  test unit                    Run unit tests only"
    echo "  test integration             Run integration tests only"
    echo "  shell                        Open an interactive bash shell"
    echo ""
    echo -e "${BOLD}Examples:${NC}"
    echo "  docker-compose run --rm setup"
    echo "  docker-compose run --rm pipeline upload --date 2026-01-15"
    echo "  docker-compose run --rm pipeline run    --date 2026-01-15"
    echo "  docker-compose run --rm pipeline run    --backfill-days 7"
    echo "  docker-compose run --rm test"
    echo "  docker-compose run --rm test unit"
}

# -----------------------------------------------------------------------------
# Main dispatcher
# -----------------------------------------------------------------------------
COMMAND="${1:-help}"
shift || true   # remove the command from $@, keep remaining args

case "$COMMAND" in
    setup)          check_env; cmd_setup  "$@" ;;
    upload)         check_env; cmd_upload "$@" ;;
    run)            check_env; cmd_run    "$@" ;;
    test)           cmd_test              "$@" ;;
    shell)          cmd_shell             "$@" ;;
    help|--help|-h) cmd_help                  ;;
    *)
        log_error "Unknown command: '$COMMAND'"
        echo ""
        cmd_help
        exit 1
        ;;
esac