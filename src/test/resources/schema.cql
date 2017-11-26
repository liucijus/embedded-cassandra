CREATE KEYSPACE items with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE items;

CREATE TABLE items(
  tenantId uuid,
  instanceId uuid,
  itemId uuid,
  commentId timeuuid,
  status text,
  created bigint,
  PRIMARY KEY ((tenantId, instanceId), itemId, commentId)
);

CREATE MATERIALIZED VIEW items_by_status AS
  SELECT tenantId, instanceId, itemId, status, commentId, created FROM items
   WHERE tenantId IS NOT NULL
     AND instanceId IS NOT NULL
     AND itemId IS NOT NULL
     AND commentId IS NOT NULL
     AND status IS NOT NULL
  PRIMARY KEY ((tenantId, instanceId), status, itemId, commentId);

