USE UDEMY;
GO

IF EXISTS (SELECT name FROM sys.objects 
		   WHERE type_desc = 'SQL_SCALAR_FUNCTION' AND
			object_id = object_id( N'stg.ConvertDurationToMinutes')
		)
DROP FUNCTION stg.ConvertDurationToMinutes
GO

CREATE FUNCTION stg.ConvertDurationToMinutes(
	@duration as nvarchar(4000)
)
/* Examples
SELECT stg.ConvertDurationToMinutes(N'309m') as q
SELECT stg.ConvertDurationToMinutes(N'56m') as q
SELECT stg.ConvertDurationToMinutes(N'7h 2m') as q
*/
RETURNS INT
AS
BEGIN
	DECLARE @Result INT = 0;

	IF LEN(@duration) = 0 OR @duration = N'' OR @duration IS NULL
	RETURN @Result

	
	DECLARE @HourPosition INT = CHARINDEX('h', @duration);
	DECLARE @Hours INT = 
		CASE WHEN @HourPosition > 0 THEN LEFT(@duration, @HourPosition-1) ELSE 0 END;

	DECLARE @MinutePosition INT = CHARINDEX('m', @duration);
	DECLARE @Minutes INT = 
		CASE WHEN @MinutePosition > 0 
		THEN SUBSTRING(@duration, @HourPosition+1, @MinutePosition - @HourPosition - 1  ) ELSE 0 END;

	SELECT @Result = @Hours * 60 + @Minutes
	
	RETURN @Result
END
GO

