#!/bin/bash

# Kafka Test Framework - Test Execution Script
# Supports Windows (Git Bash), Linux, and macOS

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "\${GREEN}====================================\${NC}"
echo -e "\${GREEN}Kafka Test Automation Framework\${NC}"
echo -e "\${GREEN}====================================\${NC}"
echo ""

# Default values
PROFILE="sequential"
THREADS=1
GROUPS=""
ENV="local"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --parallel)
            PROFILE="parallel"
            THREADS=\${2:-4}
            shift 2
            ;;
        --threads)
            THREADS=$2
            shift 2
            ;;
        --groups)
            GROUPS=$2
            shift 2
            ;;
        --env)
            ENV=$2
            shift 2
            ;;
        --help)
            echo "Usage: ./run-tests.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --parallel [N]    Run tests in parallel (default: 4 threads)"
            echo "  --threads N       Number of threads for parallel execution"
            echo "  --groups TAG      Run tests by tag (producer, consumer, smoke, etc.)"
            echo "  --env ENV         Environment (local, ci) [default: local]"
            echo "  --help            Show this help message"
            echo ""
            echo "Examples:"
            echo "  ./run-tests.sh                        # Run all tests sequentially"
            echo "  ./run-tests.sh --parallel             # Run all tests in parallel (4 threads)"
            echo "  ./run-tests.sh --parallel 8           # Run with 8 threads"
            echo "  ./run-tests.sh --groups smoke         # Run only smoke tests"
            echo "  ./run-tests.sh --groups "producer | consumer""
            exit 0
            ;;
        *)
            echo -e "\${RED}Unknown option: $1\${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Build command
CMD="mvn clean test -P\${PROFILE}"

if [ ! -z "\$GROUPS" ]; then
    CMD="\$CMD -Dgroups="\$GROUPS""
fi

if [ "\$PROFILE" == "parallel" ]; then
    CMD="\$CMD -Dthread.count=\$THREADS"
fi

CMD="\$CMD -Denv=\$ENV"

echo -e "\${YELLOW}Configuration:\${NC}"
echo "  Profile: \$PROFILE"
echo "  Threads: \$THREADS"
echo "  Groups: \${GROUPS:-all}"
echo "  Environment: \$ENV"
echo ""

echo -e "\${YELLOW}Executing:\${NC} \$CMD"
echo ""

# Execute
eval \$CMD

# Generate Allure report
if [ $? -eq 0 ]; then
    echo ""
    echo -e "\${GREEN}====================================\${NC}"
    echo -e "\${GREEN}Tests completed successfully!\${NC}"
    echo -e "\${GREEN}====================================\${NC}"
    echo ""
    echo -e "\${YELLOW}Generating Allure report...\${NC}"
    mvn allure:report
    echo ""
    echo -e "\${GREEN}Report generated!\${NC}"
    echo -e "\${YELLOW}To view report, run:\${NC} mvn allure:serve"
else
    echo ""
    echo -e "\${RED}====================================\${NC}"
    echo -e "\${RED}Tests failed!\${NC}"
    echo -e "\${RED}====================================\${NC}"
    exit 1
fi
