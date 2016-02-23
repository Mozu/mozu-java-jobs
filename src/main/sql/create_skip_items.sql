USE [Integration]
GO

/****** Object:  Table [SpringBatch].[SKIP_ITEMS]    Script Date: 10/26/2015 1:29:48 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [SpringBatch].[SKIP_ITEMS](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[jobExecutionId] [int] NULL,
	[stepExecutionId] [int] NULL,
	[jobName] [varchar](100) NULL,
	[type] [varchar](100) NULL,
	[item] [varchar](100) NULL,
	[msg] [varchar](1000) NULL,
	[runId] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO

