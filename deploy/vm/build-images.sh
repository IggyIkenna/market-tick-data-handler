#!/bin/bash

# Docker Image Build Script
# This script builds and pushes Docker images for VM deployment

set -e

# Configuration
PROJECT_ID="central-element-323112"
REGION="asia-northeast1"
REPOSITORY="market-data-tick-handler"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to show usage
show_usage() {
    echo -e "${BLUE}Docker Image Build Script${NC}"
    echo "========================="
    echo ""
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  build           - Build Docker images locally"
    echo "  push            - Push images to Google Container Registry"
    echo "  build-and-push  - Build and push images"
    echo "  clean           - Clean up local images"
    echo "  help            - Show this help"
    echo ""
    echo "Options:"
    echo "  --tag TAG       - Custom tag for images (default: latest)"
    echo "  --no-cache      - Build without using cache"
    echo "  --parallel      - Build images in parallel (faster but more resource intensive)"
    echo "  --platform ARCH - Target platform (e.g., linux/amd64, linux/arm64)"
    echo ""
    echo "Examples:"
    echo "  $0 build"
    echo "  $0 build-and-push --tag v1.0.0"
    echo "  $0 build --parallel --platform linux/amd64"
    echo "  $0 clean"
}

# Function to check prerequisites
check_prerequisites() {
    echo -e "${YELLOW}🔍 Checking prerequisites...${NC}"
    
    # Check if Docker is running
    if ! docker info > /dev/null 2>&1; then
        echo -e "${RED}❌ Docker is not running${NC}"
        exit 1
    fi
    
    # Check if gcloud is installed
    if ! command -v gcloud > /dev/null 2>&1; then
        echo -e "${RED}❌ gcloud CLI not found${NC}"
        exit 1
    fi
    
    # Check if authenticated
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
        echo -e "${RED}❌ Not authenticated with gcloud${NC}"
        echo "Please run: gcloud auth login"
        exit 1
    fi
    
    # Configure Docker for GCR
    echo -e "${YELLOW}🔧 Configuring Docker for Google Container Registry...${NC}"
    gcloud auth configure-docker "$REGION-docker.pkg.dev" --quiet
    
    echo -e "${GREEN}✅ Prerequisites check passed${NC}"
}

# Function to build images
build_images() {
    local tag="$1"
    local no_cache="$2"
    local parallel="$3"
    local platform="$4"
    
    echo -e "${BLUE}🏗️  Building Docker images${NC}"
    echo "============================="
    echo "Tag: $tag"
    echo "No Cache: $no_cache"
    echo "Parallel: $parallel"
    echo "Platform: ${platform:-default}"
    echo ""
    
    # Build function for individual images
    build_single_image() {
        local dockerfile="$1"
        local image_name="$2"
        local display_name="$3"
        
        echo -e "${YELLOW}Building $display_name...${NC}"
        local build_cmd="docker build"
        build_cmd="$build_cmd --progress=plain"
        build_cmd="$build_cmd --build-arg BUILDKIT_INLINE_CACHE=1"
        build_cmd="$build_cmd --cache-from $image_name:latest"
        build_cmd="$build_cmd --cache-from $REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/$image_name:latest"
        build_cmd="$build_cmd -f $dockerfile"
        build_cmd="$build_cmd -t $image_name:$tag"
        build_cmd="$build_cmd -t $image_name:latest"
        
        if [ -n "$platform" ]; then
            build_cmd="$build_cmd --platform $platform"
        fi
        
        build_cmd="$build_cmd ."
        
        if [ "$no_cache" = "true" ]; then
            build_cmd="$build_cmd --no-cache"
        fi
        
        echo -e "${BLUE}Command: $build_cmd${NC}"
        eval "$build_cmd"
        echo -e "${GREEN}✅ $display_name built${NC}"
    }
    
    # Build images
    if [ "$parallel" = "true" ]; then
        echo -e "${YELLOW}Building images in parallel...${NC}"
        # Build both images in parallel
        build_single_image "docker/instrument-generation/Dockerfile" "market-tick-instrument-generator" "instrument generation image" &
        build_single_image "docker/tardis-download/Dockerfile" "market-tick-tardis-downloader" "Tardis download image" &
        wait  # Wait for both builds to complete
    else
        # Build images sequentially
        build_single_image "docker/instrument-generation/Dockerfile" "market-tick-instrument-generator" "instrument generation image"
        build_single_image "docker/tardis-download/Dockerfile" "market-tick-tardis-downloader" "Tardis download image"
    fi
    
    echo ""
    echo -e "${GREEN}🎉 All images built successfully!${NC}"
    echo ""
    echo -e "${BLUE}📋 Built images:${NC}"
    echo "  - market-tick-instrument-generator:$tag"
    echo "  - market-tick-instrument-generator:latest"
    echo "  - market-tick-tardis-downloader:$tag"
    echo "  - market-tick-tardis-downloader:latest"
}

# Function to push images
push_images() {
    local tag="$1"
    
    echo -e "${BLUE}📤 Pushing Docker images${NC}"
    echo "============================"
    echo "Tag: $tag"
    echo ""
    
    # Tag and push instrument generation image
    echo -e "${YELLOW}Pushing instrument generation image...${NC}"
    docker tag "market-tick-instrument-generator:$tag" "$REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/market-tick-instrument-generator:$tag"
    docker push "$REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/market-tick-instrument-generator:$tag"
    echo -e "${GREEN}✅ Instrument generation image pushed${NC}"
    
    # Tag and push Tardis download image
    echo -e "${YELLOW}Pushing Tardis download image...${NC}"
    docker tag "market-tick-tardis-downloader:$tag" "$REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/market-tick-tardis-downloader:$tag"
    docker push "$REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/market-tick-tardis-downloader:$tag"
    echo -e "${GREEN}✅ Tardis download image pushed${NC}"
    
    echo ""
    echo -e "${GREEN}🎉 All images pushed successfully!${NC}"
    echo ""
    echo -e "${BLUE}📋 Pushed images:${NC}"
    echo "  - $REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/market-tick-instrument-generator:$tag"
    echo "  - $REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/market-tick-tardis-downloader:$tag"
}

# Function to clean up local images
clean_images() {
    echo -e "${YELLOW}🧹 Cleaning up local images...${NC}"
    
    # Remove instrument generation images
    docker rmi $(docker images "market-tick-instrument-generator" -q) 2>/dev/null || true
    docker rmi $(docker images "$REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/market-tick-instrument-generator" -q) 2>/dev/null || true
    
    # Remove Tardis download images
    docker rmi $(docker images "market-tick-tardis-downloader" -q) 2>/dev/null || true
    docker rmi $(docker images "$REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/market-tick-tardis-downloader" -q) 2>/dev/null || true
    
    echo -e "${GREEN}✅ Local images cleaned up${NC}"
}

# Main execution
case "${1:-help}" in
    build)
        shift
        tag="latest"
        no_cache="false"
        parallel="false"
        platform=""
        
        while [[ $# -gt 0 ]]; do
            case $1 in
                --tag)
                    tag="$2"
                    shift 2
                    ;;
                --no-cache)
                    no_cache="true"
                    shift
                    ;;
                --parallel)
                    parallel="true"
                    shift
                    ;;
                --platform)
                    platform="$2"
                    shift 2
                    ;;
                *)
                    echo "Unknown option: $1"
                    show_usage
                    exit 1
                    ;;
            esac
        done
        
        check_prerequisites
        build_images "$tag" "$no_cache" "$parallel" "$platform"
        ;;
    push)
        shift
        tag="latest"
        
        while [[ $# -gt 0 ]]; do
            case $1 in
                --tag)
                    tag="$2"
                    shift 2
                    ;;
                *)
                    echo "Unknown option: $1"
                    show_usage
                    exit 1
                    ;;
            esac
        done
        
        check_prerequisites
        push_images "$tag"
        ;;
    build-and-push)
        shift
        tag="latest"
        no_cache="false"
        parallel="false"
        platform=""
        
        while [[ $# -gt 0 ]]; do
            case $1 in
                --tag)
                    tag="$2"
                    shift 2
                    ;;
                --no-cache)
                    no_cache="true"
                    shift
                    ;;
                --parallel)
                    parallel="true"
                    shift
                    ;;
                --platform)
                    platform="$2"
                    shift 2
                    ;;
                *)
                    echo "Unknown option: $1"
                    show_usage
                    exit 1
                    ;;
            esac
        done
        
        check_prerequisites
        build_images "$tag" "$no_cache" "$parallel" "$platform"
        push_images "$tag"
        ;;
    clean)
        clean_images
        ;;
    help|--help|-h)
        show_usage
        ;;
    *)
        echo -e "${RED}❌ Unknown command: $1${NC}"
        show_usage
        exit 1
        ;;
esac
