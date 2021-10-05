/*
 use Test
 DROP TABLE [dbo].[artg_entry]
 DROP TABLE [dbo].[manufacturers]
 DROP TABLE [dbo].[prod__components]
 DROP TABLE [dbo].[prod__containers]
 DROP TABLE [dbo].[prod__ingredients]
 DROP TABLE [dbo].[prod__packs]
 DROP TABLE [dbo].[products]
 DROP TABLE [dbo].[sponsor]
*/
/*
 use Test
 DELETE FROM [dbo].[artg_entry]
 DELETE FROM [dbo].[manufacturers]
 DELETE FROM [dbo].[prod__components]
 DELETE FROM [dbo].[prod__containers]
 DELETE FROM [dbo].[prod__ingredients]
 DELETE FROM [dbo].[prod__packs]
 DELETE FROM [dbo].[products]
 DELETE FROM [dbo].[sponsor]
*/
use ARTG
SELECT s.name,o.name,
  ddps.row_count 
FROM sys.indexes AS i
  INNER JOIN sys.objects AS o ON i.OBJECT_ID = o.OBJECT_ID
  INNER JOIN sys.schemas AS s ON s.schema_id = o.schema_id
  INNER JOIN sys.dm_db_partition_stats AS ddps ON i.OBJECT_ID = ddps.OBJECT_ID
  AND i.index_id = ddps.index_id 
WHERE i.index_id < 2  AND o.is_ms_shipped = 0 AND s.name = 'artg'
ORDER BY s.NAME,o.NAME


--SELECT TOP (2) *
--  FROM [Test].[dbo].[artg_entry]

--SELECT TOP (2) *
--FROM [Test].[dbo].[manufacturers]

--SELECT TOP (2) *
--FROM [Test].[dbo].[products]

--SELECT TOP (2) *
--FROM [Test].[dbo].[sponsor]


