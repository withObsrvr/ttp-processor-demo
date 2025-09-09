# Git-Based Apache Arrow Schema Registry

A comprehensive guide to implementing a Git-based schema registry for Apache Arrow schemas, providing version control, compatibility checking, and multi-language support.

## Overview

This schema registry uses Git as the storage backend, providing:
- Version control and history tracking
- Branching and tagging for schema evolution
- Distributed access via Git remotes
- CI/CD integration for automated validation
- Cross-language compatibility (Go, Python, etc.)

## Repository Structure

```
arrow-schema-registry/
├── schemas/
│   ├── contract_data/
│   │   ├── v1.0.0.json
│   │   ├── v1.1.0.json
│   │   └── v2.0.0.json
│   ├── transaction_events/
│   │   └── v1.0.0.json
│   └── ledger_data/
│       └── v1.0.0.json
├── registry/
│   ├── go/
│   │   ├── client.go
│   │   ├── serialization.go
│   │   └── compatibility.go
│   └── python/
│       ├── client.py
│       ├── serialization.py
│       └── compatibility.py
├── examples/
│   ├── go/
│   └── python/
├── .github/
│   └── workflows/
│       └── validate-schemas.yml
└── README.md
```

## Schema Format

Schemas are stored as JSON files with metadata:

```json
{
  "schema_name": "contract_data",
  "version": "1.0.0",
  "compatibility_mode": "backward",
  "created_at": "2024-01-15T10:30:00Z",
  "created_by": "user@example.com",
  "description": "Stellar smart contract data changes",
  "arrow_schema": {
    "fields": [
      {
        "name": "contract_id",
        "type": "utf8",
        "nullable": false,
        "metadata": {}
      },
      {
        "name": "closed_at",
        "type": {
          "name": "timestamp",
          "unit": "MICROSECOND",
          "timezone": "UTC"
        },
        "nullable": false,
        "metadata": {}
      }
    ],
    "metadata": {
      "schema_version": "1.0",
      "schema_type": "contract_data"
    }
  }
}
```

## Go Implementation

### Registry Client

```go
// registry/go/client.go
package registry

import (
    "context"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "os"
    "path/filepath"
    "sort"
    "strings"
    "time"

    "github.com/apache/arrow/go/v17/arrow"
    "github.com/go-git/go-git/v5"
    "github.com/go-git/go-git/v5/plumbing"
    "github.com/go-git/go-git/v5/plumbing/object"
    "github.com/Masterminds/semver/v3"
)

type SchemaRegistry struct {
    repoPath   string
    repoURL    string
    repository *git.Repository
}

type SchemaMetadata struct {
    SchemaName        string                 `json:"schema_name"`
    Version           string                 `json:"version"`
    CompatibilityMode string                 `json:"compatibility_mode"`
    CreatedAt         time.Time              `json:"created_at"`
    CreatedBy         string                 `json:"created_by"`
    Description       string                 `json:"description"`
    ArrowSchema       map[string]interface{} `json:"arrow_schema"`
}

// NewSchemaRegistry creates a new schema registry instance
func NewSchemaRegistry(repoPath, repoURL string) (*SchemaRegistry, error) {
    registry := &SchemaRegistry{
        repoPath: repoPath,
        repoURL:  repoURL,
    }

    // Clone or open existing repository
    if _, err := os.Stat(repoPath); os.IsNotExist(err) {
        repo, err := git.PlainClone(repoPath, false, &git.CloneOptions{
            URL: repoURL,
        })
        if err != nil {
            return nil, fmt.Errorf("failed to clone repository: %w", err)
        }
        registry.repository = repo
    } else {
        repo, err := git.PlainOpen(repoPath)
        if err != nil {
            return nil, fmt.Errorf("failed to open repository: %w", err)
        }
        registry.repository = repo
    }

    return registry, nil
}

// GetSchema retrieves a specific version of a schema
func (r *SchemaRegistry) GetSchema(schemaName, version string) (*arrow.Schema, *SchemaMetadata, error) {
    if err := r.pull(); err != nil {
        return nil, nil, fmt.Errorf("failed to pull latest changes: %w", err)
    }

    schemaPath := filepath.Join(r.repoPath, "schemas", schemaName, fmt.Sprintf("%s.json", version))
    
    data, err := ioutil.ReadFile(schemaPath)
    if err != nil {
        return nil, nil, fmt.Errorf("schema not found: %w", err)
    }

    var metadata SchemaMetadata
    if err := json.Unmarshal(data, &metadata); err != nil {
        return nil, nil, fmt.Errorf("failed to parse schema metadata: %w", err)
    }

    schema, err := DeserializeArrowSchema(metadata.ArrowSchema)
    if err != nil {
        return nil, nil, fmt.Errorf("failed to deserialize Arrow schema: %w", err)
    }

    return schema, &metadata, nil
}

// GetLatestSchema retrieves the latest version of a schema
func (r *SchemaRegistry) GetLatestSchema(schemaName string) (*arrow.Schema, *SchemaMetadata, error) {
    versions, err := r.ListVersions(schemaName)
    if err != nil {
        return nil, nil, err
    }

    if len(versions) == 0 {
        return nil, nil, fmt.Errorf("no versions found for schema %s", schemaName)
    }

    latestVersion := versions[len(versions)-1]
    return r.GetSchema(schemaName, latestVersion)
}

// ListVersions returns all versions of a schema, sorted by semantic version
func (r *SchemaRegistry) ListVersions(schemaName string) ([]string, error) {
    if err := r.pull(); err != nil {
        return nil, fmt.Errorf("failed to pull latest changes: %w", err)
    }

    schemaDir := filepath.Join(r.repoPath, "schemas", schemaName)
    
    files, err := ioutil.ReadDir(schemaDir)
    if err != nil {
        return nil, fmt.Errorf("schema directory not found: %w", err)
    }

    var versions []*semver.Version
    for _, file := range files {
        if !file.IsDir() && strings.HasSuffix(file.Name(), ".json") {
            versionStr := strings.TrimSuffix(file.Name(), ".json")
            if version, err := semver.NewVersion(versionStr); err == nil {
                versions = append(versions, version)
            }
        }
    }

    sort.Sort(semver.Collection(versions))

    var versionStrings []string
    for _, v := range versions {
        versionStrings = append(versionStrings, v.String())
    }

    return versionStrings, nil
}

// RegisterSchema registers a new schema version
func (r *SchemaRegistry) RegisterSchema(schemaName, version string, schema *arrow.Schema, description, createdBy string) error {
    if err := r.pull(); err != nil {
        return fmt.Errorf("failed to pull latest changes: %w", err)
    }

    // Check compatibility with existing schemas
    if err := r.checkCompatibility(schemaName, version, schema); err != nil {
        return fmt.Errorf("compatibility check failed: %w", err)
    }

    // Serialize Arrow schema
    serializedSchema, err := SerializeArrowSchema(schema)
    if err != nil {
        return fmt.Errorf("failed to serialize Arrow schema: %w", err)
    }

    // Create metadata
    metadata := SchemaMetadata{
        SchemaName:        schemaName,
        Version:           version,
        CompatibilityMode: "backward", // Default to backward compatibility
        CreatedAt:         time.Now().UTC(),
        CreatedBy:         createdBy,
        Description:       description,
        ArrowSchema:       serializedSchema,
    }

    // Write schema file
    schemaDir := filepath.Join(r.repoPath, "schemas", schemaName)
    if err := os.MkdirAll(schemaDir, 0755); err != nil {
        return fmt.Errorf("failed to create schema directory: %w", err)
    }

    schemaPath := filepath.Join(schemaDir, fmt.Sprintf("%s.json", version))
    data, err := json.MarshalIndent(metadata, "", "  ")
    if err != nil {
        return fmt.Errorf("failed to marshal schema metadata: %w", err)
    }

    if err := ioutil.WriteFile(schemaPath, data, 0644); err != nil {
        return fmt.Errorf("failed to write schema file: %w", err)
    }

    // Commit changes
    return r.commitChanges(fmt.Sprintf("Add schema %s version %s", schemaName, version))
}

// checkCompatibility checks if the new schema version is compatible with existing versions
func (r *SchemaRegistry) checkCompatibility(schemaName, version string, newSchema *arrow.Schema) error {
    versions, err := r.ListVersions(schemaName)
    if err != nil || len(versions) == 0 {
        // No existing versions, compatibility check passes
        return nil
    }

    // Get the latest existing version
    latestVersion := versions[len(versions)-1]
    existingSchema, _, err := r.GetSchema(schemaName, latestVersion)
    if err != nil {
        return fmt.Errorf("failed to get existing schema: %w", err)
    }

    // Check backward compatibility
    return CheckBackwardCompatibility(existingSchema, newSchema)
}

// pull updates the local repository with remote changes
func (r *SchemaRegistry) pull() error {
    workTree, err := r.repository.Worktree()
    if err != nil {
        return err
    }

    return workTree.Pull(&git.PullOptions{
        RemoteName: "origin",
    })
}

// commitChanges commits and pushes changes to the repository
func (r *SchemaRegistry) commitChanges(message string) error {
    workTree, err := r.repository.Worktree()
    if err != nil {
        return err
    }

    // Add all changes
    if _, err := workTree.Add("."); err != nil {
        return err
    }

    // Commit changes
    _, err = workTree.Commit(message, &git.CommitOptions{
        Author: &object.Signature{
            Name:  "Schema Registry",
            Email: "registry@example.com",
            When:  time.Now(),
        },
    })
    if err != nil {
        return err
    }

    // Push changes
    return r.repository.Push(&git.PushOptions{})
}
```

### Schema Serialization

```go
// registry/go/serialization.go
package registry

import (
    "encoding/json"
    "fmt"

    "github.com/apache/arrow/go/v17/arrow"
)

// SerializeArrowSchema converts an Arrow schema to a JSON-serializable format
func SerializeArrowSchema(schema *arrow.Schema) (map[string]interface{}, error) {
    result := map[string]interface{}{
        "fields":   make([]map[string]interface{}, len(schema.Fields())),
        "metadata": make(map[string]string),
    }

    // Serialize fields
    for i, field := range schema.Fields() {
        fieldMap, err := serializeField(field)
        if err != nil {
            return nil, fmt.Errorf("failed to serialize field %s: %w", field.Name, err)
        }
        result["fields"].([]map[string]interface{})[i] = fieldMap
    }

    // Serialize metadata
    if schema.Metadata() != nil {
        for key, value := range schema.Metadata().ToMap() {
            result["metadata"].(map[string]string)[key] = value
        }
    }

    return result, nil
}

// DeserializeArrowSchema converts a JSON format back to an Arrow schema
func DeserializeArrowSchema(data map[string]interface{}) (*arrow.Schema, error) {
    fieldsData, ok := data["fields"].([]interface{})
    if !ok {
        return nil, fmt.Errorf("invalid fields data")
    }

    fields := make([]arrow.Field, len(fieldsData))
    for i, fieldData := range fieldsData {
        fieldMap, ok := fieldData.(map[string]interface{})
        if !ok {
            return nil, fmt.Errorf("invalid field data at index %d", i)
        }

        field, err := deserializeField(fieldMap)
        if err != nil {
            return nil, fmt.Errorf("failed to deserialize field at index %d: %w", i, err)
        }
        fields[i] = field
    }

    // Deserialize metadata
    var metadata arrow.Metadata
    if metadataData, ok := data["metadata"].(map[string]interface{}); ok {
        metadataMap := make(map[string]string)
        for key, value := range metadataData {
            if str, ok := value.(string); ok {
                metadataMap[key] = str
            }
        }
        metadata = arrow.MetadataFrom(metadataMap)
    }

    return arrow.NewSchema(fields, &metadata), nil
}

func serializeField(field arrow.Field) (map[string]interface{}, error) {
    fieldMap := map[string]interface{}{
        "name":     field.Name,
        "nullable": field.Nullable,
        "metadata": make(map[string]string),
    }

    // Serialize field type
    typeMap, err := serializeDataType(field.Type)
    if err != nil {
        return nil, err
    }
    fieldMap["type"] = typeMap

    // Serialize field metadata
    if field.Metadata != nil {
        for key, value := range field.Metadata.ToMap() {
            fieldMap["metadata"].(map[string]string)[key] = value
        }
    }

    return fieldMap, nil
}

func deserializeField(fieldMap map[string]interface{}) (arrow.Field, error) {
    name, ok := fieldMap["name"].(string)
    if !ok {
        return arrow.Field{}, fmt.Errorf("missing or invalid field name")
    }

    nullable, ok := fieldMap["nullable"].(bool)
    if !ok {
        nullable = true // Default to nullable
    }

    // Deserialize field type
    typeData, ok := fieldMap["type"]
    if !ok {
        return arrow.Field{}, fmt.Errorf("missing field type")
    }

    dataType, err := deserializeDataType(typeData)
    if err != nil {
        return arrow.Field{}, err
    }

    // Deserialize field metadata
    var metadata arrow.Metadata
    if metadataData, ok := fieldMap["metadata"].(map[string]interface{}); ok {
        metadataMap := make(map[string]string)
        for key, value := range metadataData {
            if str, ok := value.(string); ok {
                metadataMap[key] = str
            }
        }
        metadata = arrow.MetadataFrom(metadataMap)
    }

    return arrow.Field{
        Name:     name,
        Type:     dataType,
        Nullable: nullable,
        Metadata: metadata,
    }, nil
}

func serializeDataType(dataType arrow.DataType) (interface{}, error) {
    switch t := dataType.(type) {
    case *arrow.StringType:
        return "utf8", nil
    case *arrow.BooleanType:
        return "bool", nil
    case *arrow.Int64Type:
        return "int64", nil
    case *arrow.Uint32Type:
        return "uint32", nil
    case *arrow.TimestampType:
        return map[string]interface{}{
            "name":     "timestamp",
            "unit":     t.Unit.String(),
            "timezone": t.TimeZone,
        }, nil
    default:
        return nil, fmt.Errorf("unsupported data type: %T", dataType)
    }
}

func deserializeDataType(typeData interface{}) (arrow.DataType, error) {
    switch t := typeData.(type) {
    case string:
        switch t {
        case "utf8":
            return arrow.BinaryTypes.String, nil
        case "bool":
            return arrow.FixedWidthTypes.Boolean, nil
        case "int64":
            return arrow.PrimitiveTypes.Int64, nil
        case "uint32":
            return arrow.PrimitiveTypes.Uint32, nil
        default:
            return nil, fmt.Errorf("unsupported simple type: %s", t)
        }
    case map[string]interface{}:
        name, ok := t["name"].(string)
        if !ok {
            return nil, fmt.Errorf("missing type name")
        }

        switch name {
        case "timestamp":
            unitStr, ok := t["unit"].(string)
            if !ok {
                return nil, fmt.Errorf("missing timestamp unit")
            }

            var unit arrow.TimeUnit
            switch unitStr {
            case "MICROSECOND":
                unit = arrow.Microsecond
            case "NANOSECOND":
                unit = arrow.Nanosecond
            case "MILLISECOND":
                unit = arrow.Millisecond
            case "SECOND":
                unit = arrow.Second
            default:
                return nil, fmt.Errorf("unsupported timestamp unit: %s", unitStr)
            }

            timezone, _ := t["timezone"].(string)
            return &arrow.TimestampType{
                Unit:     unit,
                TimeZone: timezone,
            }, nil
        default:
            return nil, fmt.Errorf("unsupported complex type: %s", name)
        }
    default:
        return nil, fmt.Errorf("invalid type data: %T", typeData)
    }
}
```

### Compatibility Checking

```go
// registry/go/compatibility.go
package registry

import (
    "fmt"

    "github.com/apache/arrow/go/v17/arrow"
)

// CheckBackwardCompatibility checks if newSchema is backward compatible with oldSchema
func CheckBackwardCompatibility(oldSchema, newSchema *arrow.Schema) error {
    oldFields := make(map[string]arrow.Field)
    for _, field := range oldSchema.Fields() {
        oldFields[field.Name] = field
    }

    // Check that all old fields exist in new schema with compatible types
    for _, oldField := range oldSchema.Fields() {
        newField, exists := findField(newSchema, oldField.Name)
        if !exists {
            return fmt.Errorf("field %s removed (breaks backward compatibility)", oldField.Name)
        }

        if err := checkFieldCompatibility(oldField, newField); err != nil {
            return fmt.Errorf("field %s: %w", oldField.Name, err)
        }
    }

    return nil
}

// CheckForwardCompatibility checks if newSchema is forward compatible with oldSchema
func CheckForwardCompatibility(oldSchema, newSchema *arrow.Schema) error {
    newFields := make(map[string]arrow.Field)
    for _, field := range newSchema.Fields() {
        newFields[field.Name] = field
    }

    // Check that all new fields exist in old schema or are nullable
    for _, newField := range newSchema.Fields() {
        oldField, exists := findField(oldSchema, newField.Name)
        if !exists {
            if !newField.Nullable {
                return fmt.Errorf("new non-nullable field %s added (breaks forward compatibility)", newField.Name)
            }
        } else {
            if err := checkFieldCompatibility(newField, oldField); err != nil {
                return fmt.Errorf("field %s: %w", newField.Name, err)
            }
        }
    }

    return nil
}

func findField(schema *arrow.Schema, name string) (arrow.Field, bool) {
    for _, field := range schema.Fields() {
        if field.Name == name {
            return field, true
        }
    }
    return arrow.Field{}, false
}

func checkFieldCompatibility(oldField, newField arrow.Field) error {
    // Check nullability - new field can be more permissive (nullable)
    if !oldField.Nullable && newField.Nullable {
        // This is fine - new field allows nulls where old didn't
    } else if oldField.Nullable && !newField.Nullable {
        return fmt.Errorf("nullability changed from nullable to non-nullable")
    }

    // Check type compatibility
    return checkTypeCompatibility(oldField.Type, newField.Type)
}

func checkTypeCompatibility(oldType, newType arrow.DataType) error {
    if oldType.ID() != newType.ID() {
        return fmt.Errorf("type changed from %s to %s", oldType, newType)
    }

    // Special handling for timestamp types
    if oldTs, ok := oldType.(*arrow.TimestampType); ok {
        if newTs, ok := newType.(*arrow.TimestampType); ok {
            if oldTs.Unit != newTs.Unit {
                return fmt.Errorf("timestamp unit changed from %s to %s", oldTs.Unit, newTs.Unit)
            }
            if oldTs.TimeZone != newTs.TimeZone {
                return fmt.Errorf("timestamp timezone changed from %s to %s", oldTs.TimeZone, newTs.TimeZone)
            }
        }
    }

    return nil
}
```

## Python Implementation

### Registry Client

```python
# registry/python/client.py
import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import git
import pyarrow as pa
from semantic_version import Version

from .serialization import serialize_arrow_schema, deserialize_arrow_schema
from .compatibility import check_backward_compatibility

class SchemaMetadata:
    def __init__(self, schema_name: str, version: str, compatibility_mode: str,
                 created_at: datetime, created_by: str, description: str,
                 arrow_schema: Dict):
        self.schema_name = schema_name
        self.version = version
        self.compatibility_mode = compatibility_mode
        self.created_at = created_at
        self.created_by = created_by
        self.description = description
        self.arrow_schema = arrow_schema

    @classmethod
    def from_dict(cls, data: Dict) -> 'SchemaMetadata':
        return cls(
            schema_name=data['schema_name'],
            version=data['version'],
            compatibility_mode=data['compatibility_mode'],
            created_at=datetime.fromisoformat(data['created_at'].replace('Z', '+00:00')),
            created_by=data['created_by'],
            description=data['description'],
            arrow_schema=data['arrow_schema']
        )

    def to_dict(self) -> Dict:
        return {
            'schema_name': self.schema_name,
            'version': self.version,
            'compatibility_mode': self.compatibility_mode,
            'created_at': self.created_at.isoformat() + 'Z',
            'created_by': self.created_by,
            'description': self.description,
            'arrow_schema': self.arrow_schema
        }

class SchemaRegistry:
    def __init__(self, repo_path: str, repo_url: str):
        self.repo_path = Path(repo_path)
        self.repo_url = repo_url
        self.repository = self._init_repository()

    def _init_repository(self) -> git.Repo:
        """Initialize or clone the Git repository."""
        if self.repo_path.exists():
            return git.Repo(self.repo_path)
        else:
            return git.Repo.clone_from(self.repo_url, self.repo_path)

    def get_schema(self, schema_name: str, version: str) -> Tuple[pa.Schema, SchemaMetadata]:
        """Retrieve a specific version of a schema."""
        self._pull()
        
        schema_path = self.repo_path / "schemas" / schema_name / f"{version}.json"
        
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema {schema_name} version {version} not found")

        with open(schema_path) as f:
            data = json.load(f)

        metadata = SchemaMetadata.from_dict(data)
        schema = deserialize_arrow_schema(metadata.arrow_schema)

        return schema, metadata

    def get_latest_schema(self, schema_name: str) -> Tuple[pa.Schema, SchemaMetadata]:
        """Retrieve the latest version of a schema."""
        versions = self.list_versions(schema_name)
        if not versions:
            raise ValueError(f"No versions found for schema {schema_name}")

        latest_version = versions[-1]
        return self.get_schema(schema_name, latest_version)

    def list_versions(self, schema_name: str) -> List[str]:
        """List all versions of a schema, sorted by semantic version."""
        self._pull()
        
        schema_dir = self.repo_path / "schemas" / schema_name
        if not schema_dir.exists():
            return []

        versions = []
        for file_path in schema_dir.glob("*.json"):
            version_str = file_path.stem
            try:
                versions.append(Version(version_str))
            except ValueError:
                continue  # Skip invalid version strings

        versions.sort()
        return [str(v) for v in versions]

    def register_schema(self, schema_name: str, version: str, schema: pa.Schema,
                       description: str, created_by: str) -> None:
        """Register a new schema version."""
        self._pull()

        # Check compatibility
        self._check_compatibility(schema_name, version, schema)

        # Serialize schema
        serialized_schema = serialize_arrow_schema(schema)

        # Create metadata
        metadata = SchemaMetadata(
            schema_name=schema_name,
            version=version,
            compatibility_mode="backward",
            created_at=datetime.utcnow(),
            created_by=created_by,
            description=description,
            arrow_schema=serialized_schema
        )

        # Write schema file
        schema_dir = self.repo_path / "schemas" / schema_name
        schema_dir.mkdir(parents=True, exist_ok=True)
        
        schema_path = schema_dir / f"{version}.json"
        with open(schema_path, 'w') as f:
            json.dump(metadata.to_dict(), f, indent=2)

        # Commit changes
        self._commit_changes(f"Add schema {schema_name} version {version}")

    def _check_compatibility(self, schema_name: str, version: str, new_schema: pa.Schema) -> None:
        """Check compatibility with existing schema versions."""
        try:
            versions = self.list_versions(schema_name)
            if not versions:
                return  # No existing versions

            # Get latest version for compatibility check
            latest_version = versions[-1]
            existing_schema, _ = self.get_schema(schema_name, latest_version)

            check_backward_compatibility(existing_schema, new_schema)
        except FileNotFoundError:
            return  # No existing schema

    def _pull(self) -> None:
        """Pull latest changes from remote repository."""
        try:
            self.repository.remotes.origin.pull()
        except git.exc.GitCommandError:
            pass  # Ignore pull errors (e.g., no remote changes)

    def _commit_changes(self, message: str) -> None:
        """Commit and push changes to the repository."""
        self.repository.git.add(A=True)
        self.repository.index.commit(message)
        self.repository.remotes.origin.push()
```

### Schema Serialization

```python
# registry/python/serialization.py
from typing import Dict, Any
import pyarrow as pa

def serialize_arrow_schema(schema: pa.Schema) -> Dict[str, Any]:
    """Convert an Arrow schema to a JSON-serializable format."""
    return {
        'fields': [_serialize_field(field) for field in schema],
        'metadata': dict(schema.metadata) if schema.metadata else {}
    }

def deserialize_arrow_schema(data: Dict[str, Any]) -> pa.Schema:
    """Convert JSON format back to an Arrow schema."""
    fields = [_deserialize_field(field_data) for field_data in data['fields']]
    metadata = data.get('metadata', {})
    
    return pa.schema(fields, metadata=metadata)

def _serialize_field(field: pa.Field) -> Dict[str, Any]:
    """Serialize a single Arrow field."""
    return {
        'name': field.name,
        'type': _serialize_data_type(field.type),
        'nullable': field.nullable,
        'metadata': dict(field.metadata) if field.metadata else {}
    }

def _deserialize_field(field_data: Dict[str, Any]) -> pa.Field:
    """Deserialize a single Arrow field."""
    return pa.field(
        field_data['name'],
        _deserialize_data_type(field_data['type']),
        nullable=field_data.get('nullable', True),
        metadata=field_data.get('metadata', {})
    )

def _serialize_data_type(data_type: pa.DataType) -> Any:
    """Serialize an Arrow data type."""
    if pa.types.is_string(data_type):
        return 'utf8'
    elif pa.types.is_boolean(data_type):
        return 'bool'
    elif pa.types.is_int64(data_type):
        return 'int64'
    elif pa.types.is_uint32(data_type):
        return 'uint32'
    elif pa.types.is_timestamp(data_type):
        return {
            'name': 'timestamp',
            'unit': data_type.unit,
            'timezone': data_type.tz
        }
    else:
        raise ValueError(f"Unsupported data type: {data_type}")

def _deserialize_data_type(type_data: Any) -> pa.DataType:
    """Deserialize an Arrow data type."""
    if isinstance(type_data, str):
        if type_data == 'utf8':
            return pa.string()
        elif type_data == 'bool':
            return pa.bool_()
        elif type_data == 'int64':
            return pa.int64()
        elif type_data == 'uint32':
            return pa.uint32()
        else:
            raise ValueError(f"Unsupported simple type: {type_data}")
    elif isinstance(type_data, dict):
        if type_data['name'] == 'timestamp':
            return pa.timestamp(type_data['unit'], tz=type_data.get('timezone'))
        else:
            raise ValueError(f"Unsupported complex type: {type_data['name']}")
    else:
        raise ValueError(f"Invalid type data: {type_data}")
```

### Compatibility Checking

```python
# registry/python/compatibility.py
import pyarrow as pa

def check_backward_compatibility(old_schema: pa.Schema, new_schema: pa.Schema) -> None:
    """Check if new_schema is backward compatible with old_schema."""
    old_fields = {field.name: field for field in old_schema}
    
    # Check that all old fields exist in new schema with compatible types
    for old_field in old_schema:
        new_field = None
        for field in new_schema:
            if field.name == old_field.name:
                new_field = field
                break
        
        if new_field is None:
            raise ValueError(f"Field {old_field.name} removed (breaks backward compatibility)")
        
        _check_field_compatibility(old_field, new_field)

def _check_field_compatibility(old_field: pa.Field, new_field: pa.Field) -> None:
    """Check compatibility between two fields."""
    # Check nullability
    if not old_field.nullable and new_field.nullable:
        pass  # OK - new field allows nulls where old didn't
    elif old_field.nullable and not new_field.nullable:
        raise ValueError(f"Field {old_field.name}: nullability changed from nullable to non-nullable")
    
    # Check type compatibility
    _check_type_compatibility(old_field.name, old_field.type, new_field.type)

def _check_type_compatibility(field_name: str, old_type: pa.DataType, new_type: pa.DataType) -> None:
    """Check compatibility between two data types."""
    if old_type != new_type:
        # Special handling for timestamp types
        if pa.types.is_timestamp(old_type) and pa.types.is_timestamp(new_type):
            if old_type.unit != new_type.unit:
                raise ValueError(f"Field {field_name}: timestamp unit changed from {old_type.unit} to {new_type.unit}")
            if old_type.tz != new_type.tz:
                raise ValueError(f"Field {field_name}: timestamp timezone changed from {old_type.tz} to {new_type.tz}")
        else:
            raise ValueError(f"Field {field_name}: type changed from {old_type} to {new_type}")
```

## Usage Examples

### Go Example

```go
// examples/go/main.go
package main

import (
    "fmt"
    "log"

    "github.com/apache/arrow/go/v17/arrow"
    "github.com/apache/arrow/go/v17/arrow/memory"
    "your-org/arrow-schema-registry/registry/go"
)

func main() {
    // Initialize registry
    registry, err := registry.NewSchemaRegistry(
        "/tmp/schema-registry",
        "https://github.com/your-org/arrow-schema-registry.git",
    )
    if err != nil {
        log.Fatal(err)
    }

    // Create a new schema
    allocator := memory.NewGoAllocator()
    fields := []arrow.Field{
        {Name: "contract_id", Type: arrow.BinaryTypes.String, Nullable: false},
        {Name: "balance", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
        {Name: "closed_at", Type: &arrow.TimestampType{
            Unit: arrow.Microsecond,
            TimeZone: "UTC",
        }, Nullable: false},
    }
    schema := arrow.NewSchema(fields, nil)

    // Register the schema
    err = registry.RegisterSchema(
        "contract_data",
        "1.0.0",
        schema,
        "Initial contract data schema",
        "developer@example.com",
    )
    if err != nil {
        log.Fatal(err)
    }

    // Retrieve the schema
    retrievedSchema, metadata, err := registry.GetLatestSchema("contract_data")
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Retrieved schema: %s v%s\n", metadata.SchemaName, metadata.Version)
    fmt.Printf("Fields: %d\n", len(retrievedSchema.Fields()))
}
```

### Python Example

```python
# examples/python/main.py
import pyarrow as pa
from registry.python.client import SchemaRegistry

def main():
    # Initialize registry
    registry = SchemaRegistry(
        repo_path="/tmp/schema-registry",
        repo_url="https://github.com/your-org/arrow-schema-registry.git"
    )

    # Create a new schema
    schema = pa.schema([
        pa.field("contract_id", pa.string(), nullable=False),
        pa.field("balance", pa.int64(), nullable=True),
        pa.field("closed_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ])

    # Register the schema
    registry.register_schema(
        schema_name="contract_data",
        version="1.0.0",
        schema=schema,
        description="Initial contract data schema",
        created_by="developer@example.com"
    )

    # Retrieve the schema
    retrieved_schema, metadata = registry.get_latest_schema("contract_data")
    
    print(f"Retrieved schema: {metadata.schema_name} v{metadata.version}")
    print(f"Fields: {len(retrieved_schema)}")

if __name__ == "__main__":
    main()
```

## CI/CD Integration

### GitHub Actions Workflow

```yaml
# .github/workflows/validate-schemas.yml
name: Validate Schemas

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  validate:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v3
      with:
        python-version: '3.9'
        
    - name: Set up Go
      uses: actions/setup-go@v3
      with:
        go-version: '1.21'
    
    - name: Install Python dependencies
      run: |
        pip install pyarrow semantic-version gitpython
    
    - name: Install Go dependencies
      run: |
        cd registry/go
        go mod tidy
    
    - name: Validate schema files
      run: |
        python scripts/validate_schemas.py
    
    - name: Run compatibility tests
      run: |
        cd registry/go
        go test ./...
    
    - name: Check schema evolution
      run: |
        python scripts/check_evolution.py
```

## Best Practices

### Schema Evolution Guidelines

1. **Backward Compatibility**: New schema versions should be readable by applications using older schemas
2. **Forward Compatibility**: Older applications should handle new schema versions gracefully
3. **Field Addition**: New fields should be nullable or have default values
4. **Field Removal**: Mark fields as deprecated before removal
5. **Type Changes**: Avoid changing field types; create new fields instead

### Versioning Strategy

- Use semantic versioning (MAJOR.MINOR.PATCH)
- MAJOR: Breaking changes
- MINOR: Backward-compatible additions
- PATCH: Bug fixes, metadata updates

### Migration Path

1. **Assessment**: Analyze existing protobuf schemas
2. **Translation**: Convert to Arrow schema format
3. **Validation**: Ensure compatibility and performance
4. **Deployment**: Gradual rollout with monitoring
5. **Optimization**: Fine-tune based on usage patterns

This Git-based Apache Arrow Schema Registry provides a robust foundation for managing schema evolution in data streaming applications, offering the same benefits as protobuf registries while leveraging Arrow's superior performance for analytical workloads.