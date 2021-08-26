  SELECT min(LatestDate)
  from (
  SELECT LastUpdatedDateTime as LatestDate FROM [config].[Activities] where [ActivityID] = 1 
  UNION
  select DATEADD(day, -1,  max(StartEndTime)) AS LatestDate 
  from [CustomerIntelligence].[config].[JobLog]
  where Action = 'End'
  and Job = 'EOLHosting_DivisionStatistics_DailyChanges' 
  ) a
