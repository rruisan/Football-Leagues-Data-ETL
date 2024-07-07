-- Command to upload a file to a Snowflake stage with automatic compression
put file://{{params.path_file}} @{{params.stage}} auto_compress=true;
