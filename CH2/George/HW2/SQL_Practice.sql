--Demo DDL
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'Stock_Symbol')
	BEGIN
		PRINT 'Table Exists'
	END
Else
	Begin
		CREATE TABLE [dbo].[Stock_Symbol](
					[ID] [int] IDENTITY(1,1) NOT NULL,
					[Currency] [nvarchar](10) NULL,
					[Description] [nvarchar](255) NULL,
					[Display_Symbol] [nvarchar](255) NULL,
					[Symbol] [nvarchar](50) NULL,
					[Type] [nvarchar](255) NULL
				)
	End

IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'Company_Info')
	BEGIN
		PRINT 'Table Exists'
	END
Else
	Begin
				create table Company_Info(
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[Country] [nvarchar](10) NULL,
	[Currency] [nvarchar](10) NULL,
	[Exchange] [nvarchar](255) NULL,
	[Industry] [nvarchar](255) NULL,
	[IPO] [date] NULL,
	[logo] [nvarchar](255) NULL,
	[Market_Capitalization] [float] NULL,
	[Name] [nvarchar](255) NULL,
	[Phone] [nvarchar](255) NULL,
	[Share_Outstanding] [float] NULL,
	[Ticker] [nvarchar](50) NULL,
	[Weburl] [nvarchar](255) NULL,
	)
	End


--Select
SELECT TOP (10) [ID]
      ,[Currency]
      ,[Description]
      ,[Display_Symbol]
      ,[Symbol]
      ,[Type]
  FROM [Algo_Trade].[dbo].[Stock_Symbol]

--join
select top(20) 
      ss.[Currency]
      ,[Description]
      ,[Display_Symbol]
      ,[Symbol]
      ,[Type]
      ,[Country]
      ,ci.[Currency]
      ,[Exchange]
      ,[Industry]
      ,[IPO]
      ,[logo]
      ,[Market_Capitalization]
      ,[Name]
      ,[Phone]
      ,[Share_Outstanding]
      ,[Ticker]
      ,[Weburl]
	  from Stock_Symbol as ss left join Company_Info as ci
	  on ss.Symbol=ci.Ticker

--group by & order by
select Industry, count(*) as 'Count'
	  from Stock_Symbol as ss left join Company_Info as ci
	  on ss.Symbol=ci.Ticker
	  group by Industry
	  order by Industry

	--insert in python