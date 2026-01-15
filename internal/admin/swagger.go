/*
 * Copyright (c) 2026 Firefly Software Solutions Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package admin

// OpenAPISpec is the OpenAPI 3.0 specification for the FlyMQ Admin API.
const OpenAPISpec = `{
  "openapi": "3.0.3",
  "info": {
    "title": "FlyMQ Admin API",
    "description": "REST API for FlyMQ cluster management, monitoring, and administration.",
    "version": "1.0.0",
    "contact": {
      "name": "Firefly Software Solutions",
      "url": "https://github.com/firefly/flymq"
    },
    "license": {
      "name": "Apache 2.0",
      "url": "http://www.apache.org/licenses/LICENSE-2.0"
    }
  },
  "servers": [
    {
      "url": "/api/v1",
      "description": "Admin API v1"
    }
  ],
  "security": [],
  "tags": [
    {"name": "Cluster", "description": "Cluster management and monitoring (public)"},
    {"name": "Topics", "description": "Topic management"},
    {"name": "Consumer Groups", "description": "Consumer group management"},
    {"name": "Schemas", "description": "Schema registry management"},
    {"name": "Users", "description": "User management (admin only)"},
    {"name": "ACLs", "description": "Access control list management (admin only)"},
    {"name": "Roles", "description": "Role management (admin only)"},
    {"name": "DLQ", "description": "Dead letter queue management"},
    {"name": "Metrics", "description": "Prometheus metrics (public)"}
  ],
  "paths": {
    "/cluster": {
      "get": {
        "tags": ["Cluster"],
        "summary": "Get cluster information",
        "description": "Returns information about the FlyMQ cluster including node count, leader, and stats. This endpoint is public.",
        "operationId": "getClusterInfo",
        "responses": {
          "200": {
            "description": "Cluster information",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/ClusterInfo"}
              }
            }
          }
        }
      }
    },
    "/cluster/nodes": {
      "get": {
        "tags": ["Cluster"],
        "summary": "List cluster nodes",
        "description": "Returns a list of all nodes in the cluster. This endpoint is public.",
        "operationId": "getNodes",
        "responses": {
          "200": {
            "description": "List of nodes",
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": {"$ref": "#/components/schemas/NodeInfo"}
                }
              }
            }
          }
        }
      }
    },
    "/topics": {
      "get": {
        "tags": ["Topics"],
        "summary": "List all topics",
        "description": "Returns a list of all topics. Requires read permission.",
        "operationId": "listTopics",
        "security": [{"basicAuth": []}],
        "responses": {
          "200": {
            "description": "List of topics",
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": {"$ref": "#/components/schemas/TopicInfo"}
                }
              }
            }
          },
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"}
        }
      },
      "post": {
        "tags": ["Topics"],
        "summary": "Create a topic",
        "description": "Creates a new topic. Requires admin permission.",
        "operationId": "createTopic",
        "security": [{"basicAuth": []}],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {"$ref": "#/components/schemas/CreateTopicRequest"}
            }
          }
        },
        "responses": {
          "201": {"description": "Topic created"},
          "400": {"$ref": "#/components/responses/BadRequest"},
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"}
        }
      }
    },
    "/topics/{name}": {
      "get": {
        "tags": ["Topics"],
        "summary": "Get topic information",
        "description": "Returns detailed information about a specific topic. Requires read permission.",
        "operationId": "getTopic",
        "security": [{"basicAuth": []}],
        "parameters": [
          {"name": "name", "in": "path", "required": true, "schema": {"type": "string"}}
        ],
        "responses": {
          "200": {
            "description": "Topic information",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/TopicInfo"}
              }
            }
          },
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"},
          "404": {"$ref": "#/components/responses/NotFound"}
        }
      },
      "delete": {
        "tags": ["Topics"],
        "summary": "Delete a topic",
        "description": "Deletes a topic and all its data. Requires admin permission.",
        "operationId": "deleteTopic",
        "security": [{"basicAuth": []}],
        "parameters": [
          {"name": "name", "in": "path", "required": true, "schema": {"type": "string"}}
        ],
        "responses": {
          "204": {"description": "Topic deleted"},
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"},
          "404": {"$ref": "#/components/responses/NotFound"}
        }
      }
    },
    "/consumer-groups": {
      "get": {
        "tags": ["Consumer Groups"],
        "summary": "List consumer groups",
        "description": "Returns a list of all consumer groups. Requires read permission.",
        "operationId": "listConsumerGroups",
        "security": [{"basicAuth": []}],
        "responses": {
          "200": {
            "description": "List of consumer groups",
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": {"$ref": "#/components/schemas/ConsumerGroupInfo"}
                }
              }
            }
          },
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"}
        }
      }
    },
    "/consumer-groups/{name}": {
      "get": {
        "tags": ["Consumer Groups"],
        "summary": "Get consumer group",
        "description": "Returns information about a specific consumer group. Requires read permission.",
        "operationId": "getConsumerGroup",
        "security": [{"basicAuth": []}],
        "parameters": [
          {"name": "name", "in": "path", "required": true, "schema": {"type": "string"}}
        ],
        "responses": {
          "200": {
            "description": "Consumer group information",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/ConsumerGroupInfo"}
              }
            }
          },
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"},
          "404": {"$ref": "#/components/responses/NotFound"}
        }
      },
      "delete": {
        "tags": ["Consumer Groups"],
        "summary": "Delete consumer group",
        "description": "Deletes a consumer group. Requires admin permission.",
        "operationId": "deleteConsumerGroup",
        "security": [{"basicAuth": []}],
        "parameters": [
          {"name": "name", "in": "path", "required": true, "schema": {"type": "string"}}
        ],
        "responses": {
          "204": {"description": "Consumer group deleted"},
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"},
          "404": {"$ref": "#/components/responses/NotFound"}
        }
      }
    },
    "/users": {
      "get": {
        "tags": ["Users"],
        "summary": "List users",
        "description": "Returns a list of all users. Requires admin permission.",
        "operationId": "listUsers",
        "security": [{"basicAuth": []}],
        "responses": {
          "200": {
            "description": "List of users",
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": {"$ref": "#/components/schemas/UserInfo"}
                }
              }
            }
          },
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"}
        }
      },
      "post": {
        "tags": ["Users"],
        "summary": "Create user",
        "description": "Creates a new user. Requires admin permission.",
        "operationId": "createUser",
        "security": [{"basicAuth": []}],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {"$ref": "#/components/schemas/CreateUserRequest"}
            }
          }
        },
        "responses": {
          "201": {"description": "User created"},
          "400": {"$ref": "#/components/responses/BadRequest"},
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"}
        }
      }
    },
    "/users/{username}": {
      "get": {
        "tags": ["Users"],
        "summary": "Get user",
        "description": "Returns information about a specific user. Requires admin permission.",
        "operationId": "getUser",
        "security": [{"basicAuth": []}],
        "parameters": [
          {"name": "username", "in": "path", "required": true, "schema": {"type": "string"}}
        ],
        "responses": {
          "200": {
            "description": "User information",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/UserInfo"}
              }
            }
          },
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"},
          "404": {"$ref": "#/components/responses/NotFound"}
        }
      },
      "put": {
        "tags": ["Users"],
        "summary": "Update user",
        "description": "Updates a user's roles and/or enabled status. Requires admin permission.",
        "operationId": "updateUser",
        "security": [{"basicAuth": []}],
        "parameters": [
          {"name": "username", "in": "path", "required": true, "schema": {"type": "string"}}
        ],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {"$ref": "#/components/schemas/UpdateUserRequest"}
            }
          }
        },
        "responses": {
          "200": {"description": "User updated"},
          "400": {"$ref": "#/components/responses/BadRequest"},
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"},
          "404": {"$ref": "#/components/responses/NotFound"}
        }
      },
      "delete": {
        "tags": ["Users"],
        "summary": "Delete user",
        "description": "Deletes a user. Requires admin permission.",
        "operationId": "deleteUser",
        "security": [{"basicAuth": []}],
        "parameters": [
          {"name": "username", "in": "path", "required": true, "schema": {"type": "string"}}
        ],
        "responses": {
          "204": {"description": "User deleted"},
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"},
          "404": {"$ref": "#/components/responses/NotFound"}
        }
      }
    },
    "/users/{username}/password": {
      "post": {
        "tags": ["Users"],
        "summary": "Change password",
        "description": "Changes a user's password. Users can change their own password; admins can change any password.",
        "operationId": "changePassword",
        "security": [{"basicAuth": []}],
        "parameters": [
          {"name": "username", "in": "path", "required": true, "schema": {"type": "string"}}
        ],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {"$ref": "#/components/schemas/ChangePasswordRequest"}
            }
          }
        },
        "responses": {
          "200": {"description": "Password changed"},
          "400": {"$ref": "#/components/responses/BadRequest"},
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"}
        }
      }
    },
    "/acls": {
      "get": {
        "tags": ["ACLs"],
        "summary": "List ACLs",
        "description": "Returns a list of all topic ACLs. Requires admin permission.",
        "operationId": "listACLs",
        "security": [{"basicAuth": []}],
        "responses": {
          "200": {
            "description": "List of ACLs",
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": {"$ref": "#/components/schemas/ACLInfo"}
                }
              }
            }
          },
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"}
        }
      }
    },
    "/acls/{topic}": {
      "get": {
        "tags": ["ACLs"],
        "summary": "Get ACL",
        "description": "Returns the ACL for a specific topic. Requires admin permission.",
        "operationId": "getACL",
        "security": [{"basicAuth": []}],
        "parameters": [
          {"name": "topic", "in": "path", "required": true, "schema": {"type": "string"}}
        ],
        "responses": {
          "200": {
            "description": "ACL information",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/ACLInfo"}
              }
            }
          },
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"},
          "404": {"$ref": "#/components/responses/NotFound"}
        }
      },
      "put": {
        "tags": ["ACLs"],
        "summary": "Set ACL",
        "description": "Sets or updates the ACL for a topic. Requires admin permission.",
        "operationId": "setACL",
        "security": [{"basicAuth": []}],
        "parameters": [
          {"name": "topic", "in": "path", "required": true, "schema": {"type": "string"}}
        ],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {"$ref": "#/components/schemas/SetACLRequest"}
            }
          }
        },
        "responses": {
          "200": {"description": "ACL updated"},
          "400": {"$ref": "#/components/responses/BadRequest"},
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"}
        }
      },
      "delete": {
        "tags": ["ACLs"],
        "summary": "Delete ACL",
        "description": "Deletes the ACL for a topic. Requires admin permission.",
        "operationId": "deleteACL",
        "security": [{"basicAuth": []}],
        "parameters": [
          {"name": "topic", "in": "path", "required": true, "schema": {"type": "string"}}
        ],
        "responses": {
          "204": {"description": "ACL deleted"},
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"}
        }
      }
    },
    "/roles": {
      "get": {
        "tags": ["Roles"],
        "summary": "List roles",
        "description": "Returns a list of all available roles with their permissions. FlyMQ includes built-in roles: admin (full access), producer (write-only), consumer (read-only), and guest (public topics only). Requires admin permission.",
        "operationId": "listRoles",
        "security": [{"basicAuth": []}],
        "responses": {
          "200": {
            "description": "List of roles",
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": {"$ref": "#/components/schemas/RoleInfo"}
                },
                "example": [
                  {"name": "admin", "permissions": ["read", "write", "admin"], "description": "Full access to all operations"},
                  {"name": "producer", "permissions": ["write"], "description": "Write-only access (produce messages)"},
                  {"name": "consumer", "permissions": ["read"], "description": "Read-only access (consume messages)"}
                ]
              }
            }
          },
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"}
        }
      }
    },
    "/schemas": {
      "get": {
        "tags": ["Schemas"],
        "summary": "List schemas",
        "description": "Returns a list of all registered message schemas. Schemas can be used to validate message payloads before they are produced to a topic. Requires read permission.",
        "operationId": "listSchemas",
        "security": [{"basicAuth": []}],
        "responses": {
          "200": {
            "description": "List of schemas",
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": {"$ref": "#/components/schemas/SchemaInfo"}
                }
              }
            }
          },
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"}
        }
      },
      "post": {
        "tags": ["Schemas"],
        "summary": "Register schema",
        "description": "Registers a new message schema. Schemas define the structure of messages for a topic and can be JSON Schema, Avro, or Protobuf format. Once registered, messages can be validated against the schema before being produced. Requires admin permission.",
        "operationId": "registerSchema",
        "security": [{"basicAuth": []}],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {"$ref": "#/components/schemas/RegisterSchemaRequest"},
              "example": {
                "name": "user-events",
                "schema": "{\"type\":\"object\",\"properties\":{\"user_id\":{\"type\":\"string\"},\"action\":{\"type\":\"string\"},\"timestamp\":{\"type\":\"integer\"}},\"required\":[\"user_id\",\"action\"]}"
              }
            }
          }
        },
        "responses": {
          "201": {"description": "Schema registered successfully"},
          "400": {"$ref": "#/components/responses/BadRequest"},
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"}
        }
      }
    },
    "/schemas/{name}": {
      "get": {
        "tags": ["Schemas"],
        "summary": "Get schema",
        "description": "Returns the schema definition for a specific schema by name. Requires read permission.",
        "operationId": "getSchema",
        "security": [{"basicAuth": []}],
        "parameters": [
          {"name": "name", "in": "path", "required": true, "schema": {"type": "string"}, "description": "Schema name"}
        ],
        "responses": {
          "200": {
            "description": "Schema information",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/SchemaInfo"}
              }
            }
          },
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"},
          "404": {"$ref": "#/components/responses/NotFound"}
        }
      },
      "delete": {
        "tags": ["Schemas"],
        "summary": "Delete schema",
        "description": "Deletes a schema. Messages already produced with this schema are not affected, but new messages cannot be validated against it. Requires admin permission.",
        "operationId": "deleteSchema",
        "security": [{"basicAuth": []}],
        "parameters": [
          {"name": "name", "in": "path", "required": true, "schema": {"type": "string"}, "description": "Schema name"}
        ],
        "responses": {
          "204": {"description": "Schema deleted"},
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"},
          "404": {"$ref": "#/components/responses/NotFound"}
        }
      }
    },
    "/dlq/{topic}": {
      "get": {
        "tags": ["DLQ"],
        "summary": "Get DLQ messages",
        "description": "Returns messages from a topic's dead letter queue. Requires read permission.",
        "operationId": "getDLQMessages",
        "security": [{"basicAuth": []}],
        "parameters": [
          {"name": "topic", "in": "path", "required": true, "schema": {"type": "string"}},
          {"name": "max_messages", "in": "query", "schema": {"type": "integer", "default": 100}}
        ],
        "responses": {
          "200": {
            "description": "List of DLQ messages",
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": {"$ref": "#/components/schemas/DLQMessage"}
                }
              }
            }
          },
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"}
        }
      },
      "delete": {
        "tags": ["DLQ"],
        "summary": "Purge DLQ",
        "description": "Purges all messages from a topic's DLQ. Requires admin permission.",
        "operationId": "purgeDLQ",
        "security": [{"basicAuth": []}],
        "parameters": [
          {"name": "topic", "in": "path", "required": true, "schema": {"type": "string"}}
        ],
        "responses": {
          "204": {"description": "DLQ purged"},
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"}
        }
      }
    },
    "/dlq/{topic}/replay/{messageId}": {
      "post": {
        "tags": ["DLQ"],
        "summary": "Replay DLQ message",
        "description": "Replays a specific message from the DLQ back to the original topic. Requires admin permission.",
        "operationId": "replayDLQMessage",
        "security": [{"basicAuth": []}],
        "parameters": [
          {"name": "topic", "in": "path", "required": true, "schema": {"type": "string"}},
          {"name": "messageId", "in": "path", "required": true, "schema": {"type": "string"}}
        ],
        "responses": {
          "200": {"description": "Message replayed"},
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"},
          "404": {"$ref": "#/components/responses/NotFound"}
        }
      }
    },
    "/metrics": {
      "get": {
        "tags": ["Metrics"],
        "summary": "Get metrics",
        "description": "Returns Prometheus-compatible metrics. This endpoint is public.",
        "operationId": "getMetrics",
        "responses": {
          "200": {
            "description": "Metrics in Prometheus format",
            "content": {
              "text/plain": {
                "schema": {"type": "string"}
              }
            }
          }
        }
      }
    }
  },
  "components": {
    "securitySchemes": {
      "basicAuth": {
        "type": "http",
        "scheme": "basic",
        "description": "HTTP Basic Authentication"
      }
    },
    "responses": {
      "BadRequest": {
        "description": "Bad request",
        "content": {
          "application/json": {
            "schema": {"$ref": "#/components/schemas/Error"}
          }
        }
      },
      "Unauthorized": {
        "description": "Authentication required",
        "content": {
          "application/json": {
            "schema": {"$ref": "#/components/schemas/Error"}
          }
        }
      },
      "Forbidden": {
        "description": "Insufficient permissions",
        "content": {
          "application/json": {
            "schema": {"$ref": "#/components/schemas/Error"}
          }
        }
      },
      "NotFound": {
        "description": "Resource not found",
        "content": {
          "application/json": {
            "schema": {"$ref": "#/components/schemas/Error"}
          }
        }
      }
    },
    "schemas": {
      "Error": {
        "type": "object",
        "properties": {
          "error": {"type": "string"}
        }
      },
      "ClusterInfo": {
        "type": "object",
        "properties": {
          "cluster_id": {"type": "string"},
          "version": {"type": "string"},
          "node_count": {"type": "integer"},
          "leader_id": {"type": "string"},
          "raft_term": {"type": "integer"},
          "raft_commit_index": {"type": "integer"},
          "topic_count": {"type": "integer"},
          "total_messages": {"type": "integer"},
          "message_rate": {"type": "number"},
          "uptime": {"type": "string"},
          "nodes": {
            "type": "array",
            "items": {"$ref": "#/components/schemas/NodeInfo"}
          }
        }
      },
      "NodeInfo": {
        "type": "object",
        "properties": {
          "id": {"type": "string"},
          "address": {"type": "string"},
          "cluster_addr": {"type": "string"},
          "state": {"type": "string"},
          "raft_state": {"type": "string"},
          "is_leader": {"type": "boolean"},
          "last_seen": {"type": "string"},
          "uptime": {"type": "string"}
        }
      },
      "TopicInfo": {
        "type": "object",
        "properties": {
          "name": {"type": "string"},
          "partitions": {"type": "integer"},
          "replica_factor": {"type": "integer"},
          "message_count": {"type": "integer"},
          "retention_bytes": {"type": "integer"},
          "retention_ms": {"type": "integer"},
          "created_at": {"type": "string"}
        }
      },
      "CreateTopicRequest": {
        "type": "object",
        "required": ["name"],
        "properties": {
          "name": {"type": "string"},
          "options": {
            "type": "object",
            "properties": {
              "partitions": {"type": "integer", "default": 1},
              "replica_factor": {"type": "integer", "default": 1},
              "retention_bytes": {"type": "integer"},
              "retention_ms": {"type": "integer"}
            }
          }
        }
      },
      "ConsumerGroupInfo": {
        "type": "object",
        "properties": {
          "name": {"type": "string"},
          "topic": {"type": "string"},
          "members": {"type": "integer"},
          "state": {"type": "string"},
          "lag": {"type": "integer"}
        }
      },
      "UserInfo": {
        "type": "object",
        "properties": {
          "username": {"type": "string"},
          "roles": {"type": "array", "items": {"type": "string"}},
          "permissions": {"type": "array", "items": {"type": "string"}},
          "enabled": {"type": "boolean"},
          "created_at": {"type": "string"},
          "updated_at": {"type": "string"}
        }
      },
      "CreateUserRequest": {
        "type": "object",
        "required": ["username", "password"],
        "properties": {
          "username": {"type": "string"},
          "password": {"type": "string"},
          "roles": {"type": "array", "items": {"type": "string"}}
        }
      },
      "UpdateUserRequest": {
        "type": "object",
        "properties": {
          "roles": {"type": "array", "items": {"type": "string"}},
          "enabled": {"type": "boolean"}
        }
      },
      "ChangePasswordRequest": {
        "type": "object",
        "required": ["old_password", "new_password"],
        "properties": {
          "old_password": {"type": "string"},
          "new_password": {"type": "string"}
        }
      },
      "ACLInfo": {
        "type": "object",
        "properties": {
          "topic": {"type": "string"},
          "public": {"type": "boolean"},
          "allowed_users": {"type": "array", "items": {"type": "string"}},
          "allowed_roles": {"type": "array", "items": {"type": "string"}}
        }
      },
      "SetACLRequest": {
        "type": "object",
        "properties": {
          "public": {"type": "boolean"},
          "allowed_users": {"type": "array", "items": {"type": "string"}},
          "allowed_roles": {"type": "array", "items": {"type": "string"}}
        }
      },
      "RoleInfo": {
        "type": "object",
        "properties": {
          "name": {"type": "string"},
          "permissions": {"type": "array", "items": {"type": "string"}},
          "description": {"type": "string"}
        }
      },
      "DLQMessage": {
        "type": "object",
        "properties": {
          "id": {"type": "string"},
          "topic": {"type": "string"},
          "data": {"type": "string", "description": "Base64-encoded message data"},
          "error": {"type": "string"},
          "retries": {"type": "integer"},
          "timestamp": {"type": "string"}
        }
      },
      "SchemaInfo": {
        "type": "object",
        "properties": {
          "name": {"type": "string", "description": "Schema name/identifier"},
          "topic": {"type": "string", "description": "Associated topic (if any)"},
          "format": {"type": "string", "description": "Schema format: json, avro, protobuf"},
          "schema": {"type": "string", "description": "Schema definition as JSON string"},
          "version": {"type": "integer", "description": "Schema version number"},
          "created_at": {"type": "string", "description": "Creation timestamp"}
        }
      },
      "RegisterSchemaRequest": {
        "type": "object",
        "required": ["name", "schema"],
        "properties": {
          "name": {"type": "string", "description": "Schema name (must be unique)"},
          "schema": {"type": "string", "description": "Schema definition as JSON string"}
        }
      }
    }
  }
}`
