-- Command to copy data from a Snowflake stage to a table
copy into {{ params.table }} 
from @{{ params.stage }}/premier_positions.csv.gz 
FILE_FORMAT=(TYPE=csv field_delimiter=',' skip_header=1) 
ON_ERROR='CONTINUE';
