# Extract the resource_id, title, abstract from DSpace and populate the docs table
# DSpace 7.x
## create docs table:
CREATE TABLE docs (id SERIAL, resource_id TEXT, title TEXT, abstract TEXT, PRIMARY KEY(id));
## gin index
CREATE INDEX gin2 ON docs USING gin(to_tsvector('english', abstract));
## populate
INSERT INTO docs(resource_id, title, abstract)
SELECT DISTINCT title.dspace_object_id, title.text_value as title, abstract.text_value as abstract
FROM item i
JOIN metadatavalue abstract
ON abstract.dspace_object_id = i.uuid
JOIN metadatavalue date
ON date.dspace_object_id = i.uuid
JOIN metadatavalue title
ON title.dspace_object_id = i.uuid
JOIN metadatavalue type
ON type.dspace_object_id = i.uuid
WHERE abstract.metadata_field_id = 27
AND title.metadata_field_id = 64 AND title.text_value NOT LIKE '%.___' AND title.text_value NOT LIKE '%\_____' AND title.text_value != 'TEXT' AND title.text_value != 'LICENSE' AND title.text_value != 'ORIGINAL' AND title.text_value != 'THUMBNAIL' AND title.text_value != 'TEXT'
AND date.metadata_field_id = 15 AND date.text_value >= '2012' AND date.text_value < '2022'
AND i.in_archive = 't' AND i.withdrawn = 'f' AND i.discoverable = 't'
AND type.metadata_field_id = 66 AND type.text_value = 'Article'
# AND type.metadata_field_id = 66 AND type.text_value = 'Thesis'
# AND i.owning_collection = # lookup uuid in collection table

# DSpace 5.x
SELECT DISTINCT title.resource_id, title.text_value as title, abstract.text_value as abstract
FROM item i
JOIN metadatavalue abstract
ON abstract.resource_id = i.item_id
JOIN metadatavalue date
ON date.resource_id = i.item_id
JOIN metadatavalue title
ON title.resource_id = i.item_id
JOIN metadatavalue type
ON type.resource_id = i.item_id
WHERE abstract.metadata_field_id = 27
AND title.metadata_field_id = 64 AND title.text_value NOT LIKE '%.___' AND title.text_value NOT LIKE '%\_____' AND title.text_value != 'TEXT' AND title.text_value != 'LICENSE' AND title.text_value != 'ORIGINAL' AND title.text_value != 'THUMBNAIL' AND title.text_value != 'TEXT'
AND date.metadata_field_id = 15 AND date.text_value >= '2012' AND date.text_value < '2022'
AND i.in_archive = 't' AND i.withdrawn = 'f' AND i.discoverable = 't';
# AND type.metadata_field_id = 66 AND type.text_value = 'Thesis'
# AND type.metadata_field_id = 66 AND type.text_value = 'Article'
# AND i.owning_collection = 10


# Generate a list of titles within a date range
# DSpace 7.x
SELECT mvt.dspace_object_id, mvt.metadata_field_id, mvt.text_value AS title
FROM metadatavalue mvt
WHERE mvt.dspace_object_id IN
(SELECT mv.dspace_object_id
FROM metadatavalue mv, item it
WHERE mv.dspace_object_id = it.uuid
AND mv.metadata_field_id = 11
AND it.in_archive
AND text_value >= '2012-01'
AND text_value < '2022-01')
AND mvt.metadata_field_id = 64;

# DSpace 5.x
SELECT mvt.resource_id, mvt.metadata_field_id, mvt.text_value AS title
FROM metadatavalue mvt
WHERE mvt.resource_id IN
(SELECT mv.resource_id
FROM metadatavalue mv, item it
WHERE mv.resource_id = it.item_id
AND mv.metadata_field_id = 11
AND it.in_archive
AND text_value >= '2012-01'
AND text_value < '2022-01')
AND mvt.metadata_field_id = 64;


# Given a file listing resource ids, populate the docs table from the metadatavalue table:
# DSpace 7.x
DELETE FROM docs;
INSERT INTO docs(resource_id, title, abstract)
SELECT title.dspace_object_id, title.text_value, abstract.text_value
FROM metadatavalue title
JOIN metadatavalue abstract
ON title.dspace_object_id = abstract.dspace_object_id
WHERE title.metadata_field_id = 64
AND title.text_value NOT LIKE '%%\.___'
AND title.text_value NOT LIKE '%%\_____'
AND title.text_value != 'TEXT' AND title.text_value != 'LICENSE' AND title.text_value != 'ORIGINAL' AND title.text_value != 'THUMBNAIL' AND title.text_value != 'TEXT'
AND abstract.metadata_field_id = 27
AND title.dspace_object_id = %s;

# DSpace 5.x
DELETE FROM docs;
INSERT INTO docs(resource_id, title, doc)
SELECT title.resource_id, title.text_value, abstract.text_value
FROM metadatavalue title
JOIN metadatavalue abstract
ON title.resource_id = abstract.resource_id
WHERE title.metadata_field_id = 64
AND title.text_value NOT LIKE '%%\.___'
AND title.text_value NOT LIKE '%%\_____'
AND title.text_value != 'TEXT' AND title.text_value != 'LICENSE' AND title.text_value != 'ORIGINAL' AND title.text_value != 'THUMBNAIL' AND title.text_value != 'TEXT'
AND abstract.metadata_field_id = 27
AND title.resource_id = %s;
