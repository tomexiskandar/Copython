/*
 * copy tables into tmp schema
 */

USE [ARTG]
GO

/*
 * drop tables under tmp schema
 */
IF OBJECT_ID('tmp.artg_entry') IS NOT NULL
	BEGIN
		DROP TABLE tmp.artg_entry
	END
IF OBJECT_ID('tmp.manufacturers') IS NOT NULL
	BEGIN
		DROP TABLE tmp.manufacturers
	END
IF OBJECT_ID('tmp.products') IS NOT NULL
	BEGIN
		DROP TABLE tmp.products
	END
IF OBJECT_ID('tmp.sponsor') IS NOT NULL
	BEGIN
		DROP TABLE tmp.sponsor
	END

/*
 * copy tables
 */
SELECT *
INTO tmp.artg_entry
FROM [dbo].[artg_entry]

SELECT *
INTO tmp.manufacturers
FROM [dbo].[manufacturers]
GO

SELECT *
INTO tmp.products
FROM [dbo].[products]

SELECT *
INTO tmp.sponsor
FROM [dbo].[sponsor]
GO




