create table if not exists SS_TABLE_SANITY_CHECK
(
  ID            INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
  RUN_ID        BIGINT UNSIGNED NOT NULL,
  TABLE_NAME    VARCHAR(255) NOT NULL,
  TIMESTAMP     INTEGER NOT NULL,
  NO_OF_ROWS    INTEGER NOT NULL
) ENGINE=InnoDB;
create index SS_TABLE_SANITY_CHECK_RUN_ID_IDX on SS_TABLE_SANITY_CHECK (RUN_ID);
