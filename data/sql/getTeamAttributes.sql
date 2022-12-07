SELECT team_api_id, "date" as d, buildUpPlaySpeedClass, buildUpPlayDribblingClass, buildUpPlayPassingClass,
	buildUpPlayPositioningClass, chanceCreationPassingClass, chanceCreationCrossingClass, chanceCreationShootingClass,
	chanceCreationPositioningClass, defencePressureClass, defenceAggressionClass, defenceTeamWidthClass, defenceDefenderLineClass
	FROM Team_Attributes ta
WHERE date like '%2015%'
order by team_api_id ASC, d DESC;