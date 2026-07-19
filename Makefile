# Determine root directory
ROOT_DIR=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

# Gather all .go files for use in dependencies below
GO_FILES=$(shell find $(ROOT_DIR) -name '*.go')

# Extract Go module name from go.mod
GOMODULE=$(shell grep ^module $(ROOT_DIR)/go.mod | awk '{ print $$2 }')

# Application name
APPLICATION_NAME=dingo-operator

# Set version strings: use env vars if set, else git
VERSION ?= $(shell git describe --tags --exact-match 2>/dev/null)
COMMIT_HASH ?= $(shell git rev-parse --short HEAD)
GO_LDFLAGS=-ldflags "-s -w -X '$(GOMODULE)/internal/version.Version=$(VERSION)' -X '$(GOMODULE)/internal/version.CommitHash=$(COMMIT_HASH)'"

# Tooling
LOCALBIN ?= $(ROOT_DIR)/bin
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
CONTROLLER_TOOLS_VERSION ?= v0.17.3
ENVTEST_VERSION ?= release-0.24
ENVTEST_K8S_VERSION ?= 1.31.0

.PHONY: all build help mod-tidy clean format golines lint test manifests generate \
	install-crds uninstall-crds run image tools

all: format build ## Format and build (default)

help: ## Show this help
	@awk 'BEGIN {FS = ":.*?## "; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

mod-tidy: ## Fetch and tidy module dependencies
	go mod tidy

clean: ## Remove build artifacts
	rm -f $(APPLICATION_NAME)

format: mod-tidy ## Format source
	golangci-lint fmt
	golines -w --ignore-generated --chain-split-dots --max-len=80 --reformat-tags .

golines: ## Reformat long lines
	golines -w --ignore-generated --chain-split-dots --max-len=80 --reformat-tags .

lint: ## Run linters (golangci-lint + nilaway + modernize)
	golangci-lint run ./...
	nilaway ./...
	modernize ./...

test: manifests generate envtest ## Run tests (with envtest control plane)
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" \
		go test -v -race -coverprofile=cover.out ./...

manifests: controller-gen ## Generate CRDs and RBAC
	$(CONTROLLER_GEN) crd:generateEmbeddedObjectMeta=true rbac:roleName=dingo-operator-manager-role \
		paths=./api/... paths=./internal/... \
		output:crd:artifacts:config=config/crd/bases \
		output:rbac:artifacts:config=config/rbac

generate: controller-gen ## Generate deepcopy code
	$(CONTROLLER_GEN) object:headerFile=hack/boilerplate.go.txt paths=./api/...

install-crds: manifests ## Install CRDs into the current cluster
	kubectl apply -f config/crd/bases

uninstall-crds: ## Remove CRDs from the current cluster
	kubectl delete --ignore-not-found -f config/crd/bases

run: manifests generate ## Run the operator locally against the current kubecontext
	go run ./cmd/$(APPLICATION_NAME)

# Build the program binary
# Generated code (deepcopy) is committed, so a plain build does not regenerate.
build: mod-tidy $(GO_FILES) ## Build the operator binary
	CGO_ENABLED=0 go build \
		$(GO_LDFLAGS) \
		-o $(APPLICATION_NAME) \
		./cmd/$(APPLICATION_NAME)

# Build docker image
image: build ## Build the operator container image
	docker build -t $(APPLICATION_NAME) .

$(LOCALBIN):
	mkdir -p $(LOCALBIN)

controller-gen: $(LOCALBIN) ## Install controller-gen locally
	@test -x $(CONTROLLER_GEN) || GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

envtest: $(LOCALBIN) ## Install setup-envtest locally
	@test -x $(ENVTEST) || GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@$(ENVTEST_VERSION)

tools: controller-gen envtest ## Install all local tooling
