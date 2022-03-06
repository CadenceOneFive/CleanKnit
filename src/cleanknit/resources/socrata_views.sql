USE [tgrid4all]
GO
    /****** Object:  Schema [city_of_newyork_us]    Script Date: 12/22/2021 10:11:13 PM ******/
    CREATE SCHEMA [city_of_newyork_us]
GO
    /****** Object:  Schema [socrata]    Script Date: 12/22/2021 10:11:13 PM ******/
    CREATE SCHEMA [socrata]
GO
    /****** Object:  View [socrata].[resource_category]    Script Date: 12/22/2021 10:11:13 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO CREATE VIEW [socrata].[resource_category] AS
select r.resource_id,
    CAST(j.[key] as smallint) as category_ordinal,
    CAST(j.[value] as varchar(max)) as category
FROM socrata.resource as r
    CROSS APPLY OPENJSON(r.resource, '$.classification.categories') as j
GO
    /****** Object:  View [socrata].[resource_cooked]    Script Date: 12/22/2021 10:11:13 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO CREATE VIEW [socrata].[resource_cooked] AS -- extract out some of the commonly populated and perhaps frequently accessed fields from the resource
select r.domain,
    r.resource_id,
    r.permalink,
    -- JSON_VALUE(metadata, '$.domain') as [domain],
    j.name,
    j.id,
    j.[description],
    j.attribution,
    j.attribution_link,
    j.[type],
    j.lens_view_type,
    j.[lens_display_type],
    j.[blob_mime_type],
    j.updated_at,
    j.created_at,
    j.metadata_updated_at,
    j.data_updated_at
FROM socrata.[resource] as r
    CROSS APPLY OPENJSON(r.[resource], '$.resource') WITH (
        [name] varchar(max) '$.name',
        id varchar(16) '$.id',
        [description] varchar(max) '$.description',
        [attribution] varchar(max) '$.attribution',
        [attribution_link] varchar(max) '$.attribution_link',
        [type] varchar(64) '$.type',
        [lens_view_type] varchar(64) '$.lens_view_type',
        [lens_display_type] varchar(64) '$.lens_display_type',
        [blob_mime_type] varchar(128) '$.blob_mime_type',
        [updated_at] datetime '$.updatedAt',
        [created_at] datetime '$.createdAt',
        [metadata_updated_at] datetime '$.metadata_updated_at',
        [data_updated_at] datetime '$.data_updated_at',
        [publication_date] datetime '$.publication_date'
    ) as j
GO
    /****** Object:  View [socrata].[resource_domain_tag]    Script Date: 12/22/2021 10:11:13 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO CREATE VIEW [socrata].[resource_domain_tag] AS
select r.resource_id,
    CAST(j.[key] as smallint) as domain_tag_ordinal,
    CAST(j.[value] as varchar(max)) as domain_tag
FROM socrata.resource as r
    CROSS APPLY OPENJSON(r.resource, '$.classification.domain_tags') as j
GO
    /****** Object:  View [socrata].[resource_page_view]    Script Date: 12/22/2021 10:11:13 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO CREATE VIEW [socrata].[resource_page_view] AS
select r.domain,
    r.resource_id,
    u.[updated_at],
    j.page_views_last_week,
    j.page_views_last_month,
    j.page_views_total
FROM socrata.resource as r
    CROSS APPLY OPENJSON(r.resource, '$.resource') WITH (
        [updated_at] datetime '$.updatedAt',
        [created_at] datetime '$.createdAt',
        [metadata_updated_at] datetime '$.metadata_updated_at',
        [data_updated_at] datetime '$.data_updated_at'
    ) as u
    CROSS APPLY OPENJSON(r.resource, '$.resource.page_views') WITH (
        page_views_last_week integer '$.page_views_last_week',
        page_views_last_month integer '$.page_views_last_month',
        page_views_total integer '$.page_views_total'
    ) AS j
GO