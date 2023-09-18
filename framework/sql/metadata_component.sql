--Create schema
CREATE SCHEMA  IF NOT EXISTS UPR_DATA_INTG_META.CONFIG;

--Create Internal stage
CREATE STAGE IF NOT EXISTS UPR_DATA_INTG_META.CONFIG.METADATA_SCRIPTS_STAGE;

-- Place the metadata processing scripts in internal stage
put file://&FILE_HOME_PATH/src/metadata_processor/* @metadata_scripts_stage auto_compress = false overwrite = true;