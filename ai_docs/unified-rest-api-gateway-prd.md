# Product Requirements Document: Unified REST API Gateway

## 1. Overview

### 1.1 Product Purpose
The Unified REST API Gateway provides a standardized REST interface to Obsrvr's catalog of blockchain data processors. It serves as a single integration point for customers, abstracting away the complexity of gRPC communication with individual processors and providing a consistent, developer-friendly API experience.

### 1.2 User Personas

#### Enterprise Blockchain Data Consumer
- Financial institutions, analytics providers, and compliance teams
- Needs reliable, scalable access to processed blockchain data
- Prefers familiar REST APIs over gRPC
- Values documentation, consistency, and reliability

#### Blockchain Developer
- Building applications on top of Stellar/other blockchain data
- Needs programmatic access to specific blockchain events
- Wants simplified integration and developer experience
- Prefers language-specific SDKs and examples

#### Internal Service Team
- Building other Obsrvr services that consume processor data
- Needs both REST and streaming capabilities
- Requires service discovery and health monitoring

## 2. Core Features

### 2.1 Unified API Surface

#### 2.1.1 Request Routing
- Dynamic routing to appropriate processors based on endpoint and parameters
- Transparent processor selection logic
- Support for processor versioning
- Fallback mechanisms when processors are unavailable

#### 2.1.2 Response Formatting
- Consistent JSON response structure across all endpoints
- Standardized error formats and codes
- Support for different output formats (JSON, CSV, etc.)
- Pagination standardization across all collection endpoints

### 2.2 Processor Integration

#### 2.2.1 Processor Registry
- Self-registration mechanism for processors
- Capability advertisement (supported methods, data types)
- Health status monitoring
- Load balancing across processor instances

#### 2.2.2 Adapter System
- Protocol adapters for different processor types
- Request/response transformation
- Streaming data normalization
- Caching layer for frequent requests

### 2.3 API Access Patterns

#### 2.3.1 REST Endpoints
- Full RESTful API following best practices
- Resource-oriented design
- HTTP verbs used appropriately
- Consistent URL structure

#### 2.3.2 Real-time Data Access
- WebSocket support for live data streaming
- Server-Sent Events alternative
- Connection management and recovery
- Filtered subscriptions

### 2.4 Developer Experience

#### 2.4.1 Documentation
- OpenAPI/Swagger specifications for all endpoints
- Interactive API explorer
- Comprehensive guides with examples
- SDK usage documentation

#### 2.4.2 Client SDKs
- Official SDKs for popular languages:
  - JavaScript/TypeScript
  - Python
  - Go
  - Java
- Consistent client interface across languages
- Robust error handling and retry logic

### 2.5 Enterprise Features

#### 2.5.1 Authentication & Authorization
- API key authentication
- JWT support
- OAuth 2.0 integration option
- Role-based access control for endpoints

#### 2.5.2 Rate Limiting & Quotas
- Configurable rate limits per customer
- Usage quotas and metering
- Throttling policies
- Rate limit headers in responses

## 3. Technical Requirements

### 3.1 Performance

#### 3.1.1 Latency
- Maximum 100ms added latency over direct processor access
- 99th percentile response times under 500ms for non-streaming requests
- Caching strategy for frequently accessed data

#### 3.1.2 Throughput
- Support for 1000+ concurrent client connections
- Ability to handle 10,000+ requests per second
- Efficient connection pooling to processors

### 3.2 Reliability

#### 3.2.1 Fault Tolerance
- Circuit breaker pattern for processor connections
- Graceful degradation when processors are unavailable
- Request retries with exponential backoff

#### 3.2.2 Availability
- 99.9% uptime SLA
- Regional deployment support
- Blue/green deployment capability

### 3.3 Scalability

#### 3.3.1 Horizontal Scaling
- Stateless design for API gateway instances
- Auto-scaling based on load metrics
- Connection load balancing

#### 3.3.2 Multi-Tenant Resource Optimization
- Tenant-aware connection pooling to processors
- Resource segregation to prevent noisy neighbor problems
- Efficient handling of long-lived connections
- Connection multiplexing to processors
- Minimal memory footprint per connection
- Scaling based on tenant and pipeline activity patterns

### 3.4 Observability

#### 3.4.1 Logging
- Structured logging format with tenant context
- Request/response logging with sampling
- Error details for troubleshooting
- Tenant-specific log filters and access controls

#### 3.4.2 Metrics
- Request volume, latency, and error rates
- Processor connection health
- Resource utilization
- Client usage patterns
- Per-tenant and per-pipeline metrics dashboards

#### 3.4.3 Tracing
- Distributed tracing across gateway and processors
- Unique request IDs for correlation
- Performance bottleneck identification

### 3.5 Security

#### 3.5.1 Data Protection
- TLS encryption for all connections
- Sensitive data handling policies
- Input validation and sanitization

#### 3.5.2 Access Control
- API key management system
- IP address restrictions option
- Audit logging for security events

## 4. User Experience

### 4.1 API Design Principles

#### 4.1.1 Consistency
- Uniform parameter naming across endpoints
- Consistent pluralization in resource naming
- Standard error format and codes

#### 4.1.2 Discoverability
- HATEOAS links where appropriate
- Self-documenting responses
- Clear parameter descriptions

### 4.2 Developer Onboarding

#### 4.2.1 Quick Start
- Getting started guides for common use cases
- Authentication setup walkthrough
- Sample applications with source code

#### 4.2.2 Integration Support
- Comprehensive troubleshooting guides
- Integration patterns for common scenarios
- Code snippets for typical operations

## 5. Deployment and Operations

### 5.1 Multi-Tenant Architecture

#### 5.1.1 Hybrid Approach for Obsrvr Flow
- Single unified gateway with logical tenant partitioning
- URL structure: `/v1/tenants/{tenant_id}/pipelines/{pipeline_id}/resources`
- One gateway service serving all tenants and pipelines
- Dedicated connection pools per tenant to processors

#### 5.1.2 Tenant Isolation
- Request authentication bound to tenant identity
- Separate rate limits and quotas per tenant
- Tenant-specific processor routing
- Resource isolation to prevent noisy neighbor problems
- Audit logs separated by tenant

### 5.2 Infrastructure

#### 5.2.1 Deployment Model
- Kubernetes-based deployment
- Containerized microservice architecture
- Infrastructure as code templates
- Horizontal scaling to handle multi-tenant load

#### 5.2.2 Networking
- Ingress controller configuration
- Service mesh integration option
- Network policy recommendations
- Traffic segmentation for multi-tenant requests

### 5.3 CI/CD

#### 5.2.1 Build Pipeline
- Automated testing for each API endpoint
- Performance regression testing
- Security scanning

#### 5.2.2 Deployment Strategy
- Canary deployments for new versions
- Automated rollback capability
- Feature flags for controlled rollout

### 5.4 Monitoring and Alerting

#### 5.3.1 Health Monitoring
- Comprehensive health check endpoints
- Processor connection status dashboard
- Alerting for service degradation

#### 5.3.2 Performance Monitoring
- Real-time performance dashboards
- Historical trend analysis
- Anomaly detection

## 6. API Endpoints

### 6.1 Core Endpoints

#### 6.1.1 Multi-Tenant Data Retrieval
- `GET /v1/tenants/{tenant_id}/pipelines/{pipeline_id}/ledgers` - List ledgers with filtering
- `GET /v1/tenants/{tenant_id}/pipelines/{pipeline_id}/ledgers/{sequence}` - Get specific ledger
- `GET /v1/tenants/{tenant_id}/pipelines/{pipeline_id}/transactions` - List transactions with filtering
- `GET /v1/tenants/{tenant_id}/pipelines/{pipeline_id}/transactions/{hash}` - Get specific transaction
- `GET /v1/tenants/{tenant_id}/pipelines/{pipeline_id}/accounts/{account_id}/transactions` - Get account transactions
- `GET /v1/tenants/{tenant_id}/pipelines/{pipeline_id}/events` - Query events with filtering
- `GET /v1/tenants/{tenant_id}/pipelines/{pipeline_id}/token-transfers` - List token transfers with filtering

#### 6.1.2 Multi-Tenant Streaming Endpoints
- `GET /v1/tenants/{tenant_id}/pipelines/{pipeline_id}/streams/ledgers` - Stream ledgers (WebSocket)
- `GET /v1/tenants/{tenant_id}/pipelines/{pipeline_id}/streams/transactions` - Stream transactions (WebSocket)
- `GET /v1/tenants/{tenant_id}/pipelines/{pipeline_id}/streams/events` - Stream events (WebSocket)
- `GET /v1/tenants/{tenant_id}/pipelines/{pipeline_id}/streams/token-transfers` - Stream token transfers (WebSocket)

### 6.2 Administrative Endpoints

#### 6.2.1 System Status
- `GET /v1/status` - Overall gateway status
- `GET /v1/processors` - List connected processors and their status
- `GET /v1/health` - Health check endpoint

#### 6.2.2 Tenant Management
- `GET /v1/tenants/{tenant_id}` - Tenant information
- `GET /v1/tenants/{tenant_id}/pipelines` - List pipelines for tenant
- `GET /v1/tenants/{tenant_id}/usage` - Tenant usage metrics
- `GET /v1/tenants/{tenant_id}/limits` - Tenant rate limits and quotas

## 7. Implementation Phases

### 7.1 Phase 1: Core Multi-Tenant API Gateway (2 months)
- Basic routing infrastructure with tenant/pipeline partitioning
- Support for ledger-jsonrpc-processor
- Tenant-aware key authentication
- Core data retrieval endpoints with tenant context
- Multi-tenant connection management
- Initial documentation

### 7.2 Phase 2: Streaming & Advanced Features (2 months)
- WebSocket implementation
- Token transfer processor integration
- SDK for JavaScript/TypeScript
- Enhanced documentation
- Caching layer

### 7.3 Phase 3: Enterprise Features (2 months)
- Advanced authentication options
- Rate limiting and quotas
- Additional SDKs (Python, Go)
- Enterprise dashboards
- Enhanced monitoring

### 7.4 Phase 4: Ecosystem Expansion (Ongoing)
- Support for additional processors
- Additional data formats
- Advanced analytics endpoints
- Partner-specific customizations

## 8. Success Metrics

### 8.1 Adoption Metrics
- Number of active API users
- Requests per day
- SDK downloads
- Documentation page views

### 8.2 Performance Metrics
- Average response time
- Error rates
- System uptime
- Customer-reported issues

### 8.3 Business Metrics
- Customer retention
- API usage growth
- Support ticket volume
- Feature request frequency

## 9. Open Questions and Considerations

### 9.1 Technical Considerations
- How to handle version compatibility between processors and the gateway?
- What strategies for caching blockchain data are most effective?
- How to balance real-time data needs with system performance?

### 9.2 Business Considerations
- Pricing model for different API access levels
- Enterprise SLA requirements
- Competitive differentiation strategy
- Partnership opportunities

## 10. Appendix

### 10.1 Glossary
- **Processor**: A service that transforms raw blockchain data into specific formats
- **Gateway**: The unified REST API service that routes requests to processors
- **Adapter**: Component that translates between gateway and processor protocols
- **WebSocket**: Protocol providing full-duplex communication for streaming data

### 10.2 Related Systems
- Ledger JSON-RPC Processor
- Token Transfer Processor (TTP)
- Stellar Live Source
- Flow Control Plane

### 10.3 API Examples

```
# Example multi-tenant REST request
GET /v1/tenants/tenant123/pipelines/pipeline456/ledgers?start_sequence=40000000&limit=10

# Example response
{
  "data": [
    {
      "sequence": 40000000,
      "hash": "7d3...",
      "closed_at": "2023-04-01T12:00:00Z",
      "transaction_count": 12,
      "operation_count": 24
    },
    ...
  ],
  "pagination": {
    "next_cursor": "40000010",
    "limit": 10,
    "order": "asc"
  }
}
```

```
# Example multi-tenant WebSocket connection
WS /v1/tenants/tenant123/pipelines/pipeline456/streams/token-transfers?asset=native&min_amount=1000

# Example message
{
  "type": "transfer",
  "ledger_sequence": 40000015,
  "tx_hash": "abc...",
  "from": "GDXH...",
  "to": "GABX...",
  "asset": "native",
  "amount": "5000.0"
}
```