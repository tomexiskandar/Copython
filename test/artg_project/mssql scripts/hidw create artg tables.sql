USE [ARTG]
GO
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/****************************
 * create a schema called artg
 ***************************/
IF (NOT EXISTS(SELECT *
        FROM [INFORMATION_SCHEMA].[SCHEMATA]
		WHERE SCHEMA_NAME = 'artg'))
BEGIN
	EXEC ('CREATE SCHEMA [artg] AUTHORIZATION [dbo]');
END



/****************************
 * create table lic__conditions
 ***************************/
IF (EXISTS(SELECT *
           FROM INFORMATION_SCHEMA.TABLES
		   WHERE TABLE_SCHEMA = 'artg'
		   AND TABLE_NAME = 'lic__conditions'))
BEGIN
	DROP TABLE [artg].[lic__conditions]
END

CREATE TABLE [artg].[lic__conditions](
	[Results] [int] NULL,
	[Conditions] [int] NULL,
	[__item] [varchar](max) NULL,
	[LicenceId] [int] NULL,
) ON [PRIMARY]


/****************************
 * create table lic__manufacturers
 ***************************/
IF (EXISTS(SELECT *
           FROM INFORMATION_SCHEMA.TABLES
		   WHERE TABLE_SCHEMA = 'artg'
		   AND TABLE_NAME = 'lic__manufacturers'))
BEGIN
	DROP TABLE [artg].[lic__manufacturers]
END

CREATE TABLE [artg].[lic__manufacturers](
	[Results] [int] NULL,
	[Manufacturers] [int] NULL,
	[AddressLine1] [nvarchar](200) NULL,
	[AddressLine2] [nvarchar](200) NULL,
	[Country] [nvarchar](200) NULL,
	[Postcode] [nvarchar](200) NULL,
	[State] [nvarchar](200) NULL,
	[Suburb] [nvarchar](200) NULL,
	[Name] [nvarchar](200) NULL,
	[LicenceId] [int] NULL,
) ON [PRIMARY]



/****************************
 * create table lic__products
 ***************************/
IF (EXISTS(SELECT *
           FROM INFORMATION_SCHEMA.TABLES
		   WHERE TABLE_SCHEMA = 'artg'
		   AND TABLE_NAME = 'lic__products'))
BEGIN
	DROP TABLE [artg].[lic__products]
END

CREATE TABLE [artg].[lic__products](
	[Results] [int] NULL,
	[Products] [int] NULL,
	[AdditionalInformation] [varchar](max) NULL, -- to be monitored over time. ARTG specifies the type as list.
	[EffectiveDate] [nvarchar](200) NULL,
	[GMDNCode] [nvarchar](200) NULL,
	[GMDNTerm] [nvarchar](200) NULL,
	[Name] [nvarchar](200) NULL,
	[SpecificIndications] [varchar] (max) NULL,   -- to be monitored over time. ARTG specifies the type as list.
	[Type] [nvarchar](200) NULL,
	[LicenceId] [int] NULL,
) ON [PRIMARY]



/****************************
 * create table prod__component
 ***************************/
IF (EXISTS(SELECT *
           FROM INFORMATION_SCHEMA.TABLES
		   WHERE TABLE_SCHEMA = 'artg'
		   AND TABLE_NAME = 'prod__components'))
BEGIN
	DROP TABLE [artg].[prod__components]
END

CREATE TABLE [artg].[prod__components](
	[Results] [int] NULL,
	[Products] [int] NULL,
	[Components] [int] NULL,
	[DosageForm] [nvarchar](200) NULL,
	[RouteOfAdministration] [nvarchar](200) NULL,
	[VisualIdentification] [varchar](max) NULL,
	[LicenceId] [int] NULL,
	[product_name] [nvarchar] (200),
) ON [PRIMARY]


/****************************
 * create table prod__containers
 ***************************/
IF (EXISTS(SELECT *
           FROM INFORMATION_SCHEMA.TABLES
		   WHERE TABLE_SCHEMA = 'artg'
		   AND TABLE_NAME = 'prod__containers'))
BEGIN
	DROP TABLE [artg].[prod__containers]
END

CREATE TABLE [artg].[prod__containers](
	[Results] [int] NULL,
	[Products] [int] NULL,
	[Containers] [int] NULL,
	[Closure] [nvarchar](200) NULL,
	[Conditions] [nvarchar](200) NULL,
	[LifeTime] [nvarchar](200) NULL,
	[Material] [nvarchar](200) NULL,
	[Temperature] [nvarchar](200) NULL,
	[Type] [nvarchar](200) NULL,
	[LicenceId] [int] NULL,
	[product_name] [nvarchar] (200),
) ON [PRIMARY]



/****************************
 * create table prod__ingredients
 ***************************/
IF (EXISTS(SELECT *
           FROM INFORMATION_SCHEMA.TABLES
		   WHERE TABLE_SCHEMA = 'artg'
		   AND TABLE_NAME = 'prod__ingredients'))
BEGIN
	DROP TABLE [artg].[prod__ingredients]
END

CREATE TABLE [artg].[prod__ingredients](
	[Results] [int] NULL,
	[Products] [int] NULL,
	[Ingredients] [int] NULL,
	[Name] [nvarchar](200) NULL,
	[Strength] [nvarchar](200) NULL,
	[LicenceId] [int] NULL,
	[product_name] [nvarchar] (200),
) ON [PRIMARY]



/****************************
 * create table prod__packs
 ***************************/
IF (EXISTS(SELECT *
           FROM INFORMATION_SCHEMA.TABLES
		   WHERE TABLE_SCHEMA = 'artg'
		   AND TABLE_NAME = 'prod__packs'))
BEGIN
	DROP TABLE [artg].[prod__packs]
END

CREATE TABLE [artg].[prod__packs](
	[Results] [int] NULL,
	[Products] [int] NULL,
	[Packs] [int] NULL,
	[PoisonSchedule] [nvarchar](200) NULL,
	[Size] [nvarchar](200) NULL,
	[LicenceId] [int] NULL,
	[product_name] [nvarchar] (200),
) ON [PRIMARY]



/****************************
 * create table prod__specific_indications
 ***************************/
IF (EXISTS(SELECT *
           FROM INFORMATION_SCHEMA.TABLES
		   WHERE TABLE_SCHEMA = 'artg'
		   AND TABLE_NAME = 'prod__specific_indications'))
BEGIN
	DROP TABLE [artg].[prod__specific_indications]
END

CREATE TABLE [artg].[prod__specific_indications](
	[Results] [int] NULL,
	[Products] [int] NULL,
	[SpecificIndications] [int] NULL,
	[__item] [varchar](max) NULL,
	[LicenceId] [int] NULL,
	[product_name] [nvarchar] (200),
) ON [PRIMARY]



/****************************
 * create table prod__warnings
 ***************************/
IF (EXISTS(SELECT *
           FROM INFORMATION_SCHEMA.TABLES
		   WHERE TABLE_SCHEMA = 'artg'
		   AND TABLE_NAME = 'prod__warnings'))
BEGIN
	DROP TABLE [artg].[prod__warnings]
END

CREATE TABLE [artg].[prod__warnings](
	[Results] [int] NULL,
	[Products] [int] NULL,
	[Warnings] [int] NULL,
	[__item] [varchar](max) NULL,
	[LicenceId] [int] NULL,
	[product_name] [nvarchar] (200),
) ON [PRIMARY]
GO




/****************************
 * create table results_licences
 ***************************/
IF (EXISTS(SELECT *
           FROM INFORMATION_SCHEMA.TABLES
		   WHERE TABLE_SCHEMA = 'artg'
		   AND TABLE_NAME = 'results_lic'))
BEGIN
	DROP TABLE [artg].[results_lic]
END

CREATE TABLE [artg].[results_lic](
	[Results] [int] NULL,
	[AnnualChargeExemptWaverDate] [nvarchar] (200) NULL,
	[AnnualChargeExemptWaverFlag] [nvarchar] (200) NULL,
	[BlackTriangleSchemeFlag] [char] (1) NULL,
	[ApprovalArea] [nvarchar](200) NULL,
	[ConsumerInfo_DocLink] [nvarchar](2000) NULL,
	[EntryType] [nvarchar](200) NULL,
	[LicenceClass] [nvarchar](200) NULL,
	[LicenceId] [int] NOT NULL,
	[Name] [nvarchar](200) NULL,
	[ProductCategory] [nvarchar](200) NULL,
	[ProductInfo_DocLink] [nvarchar](2000) NULL,
	/* add sponsor */
	[SprAddressLine1] [nvarchar] (200) NULL,
	[SprAddressLine2] [nvarchar] (200) NULL,
	[SprCountry] [nvarchar] (200) NULL,
	[SprPostcode] [int] NULL,
	[SprState] [nvarchar] (200) NULL,
	[SprSuburb] [nvarchar] (200) NULL,
	[SprName] [nvarchar] (200) NULL,
	/* end adding sponsor */
	[StartDate] [date] NULL,
	[Status] [nvarchar](200) NULL,
	[created_on_utc] [smalldatetime] NULL,
	[data_source] [nvarchar](200) NULL,
	[params_template] [nvarchar](200) NULL,
	[params] [nvarchar](200) NULL
) ON [PRIMARY]


--/*
-- * add primary key
-- */

--ALTER TABLE [artg].[results_lic]
--ADD CONSTRAINT PK_results_lic_LicenceId PRIMARY KEY CLUSTERED (LicenceId);


--/*
-- * add foreign key
-- */
--ALTER TABLE [artg].[lic__conditions]
--ADD CONSTRAINT FK_lic__artg_conditions_LicenceId_REF_artg_results_lic_LicenceId FOREIGN KEY (LicenceId)
--	REFERENCES [artg].[results_lic] (LicenceId)
--	ON DELETE CASCADE
--	ON UPDATE CASCADE;

--ALTER TABLE [artg].[lic__manufacturers]
--ADD CONSTRAINT FK_lic__artg_manufacturers_LicenceId_REF_artg_results_lic_LicenceId FOREIGN KEY (LicenceId)
--	REFERENCES [artg].[results_lic] (LicenceId)
--	ON DELETE CASCADE
--	ON UPDATE CASCADE;

--ALTER TABLE [artg].[lic__products]
--ADD CONSTRAINT FK_lic__artg_products_LicenceId_REF_artg_results_lic_LicenceId FOREIGN KEY (LicenceId)
--	REFERENCES [artg].[results_lic] (LicenceId)
--	ON DELETE CASCADE
--	ON UPDATE CASCADE;

--ALTER TABLE [artg].[prod__components]
--ADD CONSTRAINT FK_lic__artg_prod__components_LicenceId_REF_artg_results_lic_LicenceId FOREIGN KEY (LicenceId)
--	REFERENCES [artg].[results_lic] (LicenceId)
--	ON DELETE CASCADE
--	ON UPDATE CASCADE;

--ALTER TABLE [artg].[prod__containers]
--ADD CONSTRAINT FK_lic__artg_prod__containers_LicenceId_REF_artg_results_lic_LicenceId FOREIGN KEY (LicenceId)
--	REFERENCES [artg].[results_lic] (LicenceId)
--	ON DELETE CASCADE
--	ON UPDATE CASCADE;

--ALTER TABLE [artg].[prod__ingredients]
--ADD CONSTRAINT FK_lic__artg_prod__ingredients_LicenceId_REF_artg_results_lic_LicenceId FOREIGN KEY (LicenceId)
--	REFERENCES [artg].[results_lic] (LicenceId)
--	ON DELETE CASCADE
--	ON UPDATE CASCADE;

--ALTER TABLE [artg].[prod__packs]
--ADD CONSTRAINT FK_lic__artg_prod__packs_LicenceId_REF_artg_results_lic_LicenceId FOREIGN KEY (LicenceId)
--	REFERENCES [artg].[results_lic] (LicenceId)
--	ON DELETE CASCADE
--	ON UPDATE CASCADE;

--ALTER TABLE [artg].[prod__specific_indications]
--ADD CONSTRAINT FK_lic__artg_prod__specific_indications_LicenceId_REF_artg_results_lic_LicenceId FOREIGN KEY (LicenceId)
--	REFERENCES [artg].[results_lic] (LicenceId)
--	ON DELETE CASCADE
--	ON UPDATE CASCADE;

--ALTER TABLE [artg].[prod__warnings]
--ADD CONSTRAINT FK_lic__artg_prod__warnings_LicenceId_REF_artg_results_lic_LicenceId FOREIGN KEY (LicenceId)
--	REFERENCES [artg].[results_lic] (LicenceId)
--	ON DELETE CASCADE
--	ON UPDATE CASCADE;










