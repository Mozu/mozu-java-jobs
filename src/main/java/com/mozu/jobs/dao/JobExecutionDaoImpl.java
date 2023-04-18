/*
 * COPYRIGHT (C) 2014 Volusion Inc. All Rights Reserved.
 */
package com.mozu.jobs.dao;

import org.springframework.batch.core.repository.dao.AbstractJdbcBatchMetadataDao;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

public class JobExecutionDaoImpl extends AbstractJdbcBatchMetadataDao implements JobExecutionDao {

    private Boolean isSql;

    //sql queries
    public static final String JOB_EXECUTION_BY_TENANT_MS = "SELECT TOP 20 ex.JOB_EXECUTION_ID "
                                                    + "from SpringBatch.BATCH_JOB_EXECUTION ex  "
                                                    + "JOIN SpringBatch.BATCH_JOB_EXECUTION_PARAMS param on param.JOB_EXECUTION_ID = ex.JOB_EXECUTION_ID AND param.KEY_NAME = 'tenantId' and param.LONG_VAL = ? "
                                                    + "JOIN SpringBatch.BATCH_JOB_INSTANCE inst on inst.JOB_INSTANCE_ID = ex.JOB_INSTANCE_ID AND inst.JOB_NAME = ? "
                                                    + "ORDER BY JOB_EXECUTION_ID DESC";

    public static final String LAST_SUCCESSFUL_EXECUTION_DATE_MS = "SELECT TOP 1 ex.START_TIME "
            + "from SpringBatch.BATCH_JOB_EXECUTION ex  "
            + "JOIN SpringBatch.BATCH_JOB_EXECUTION_PARAMS param on param.JOB_EXECUTION_ID = ex.JOB_EXECUTION_ID AND param.KEY_NAME = 'tenantId' and param.LONG_VAL = ? "
            + "JOIN SpringBatch.BATCH_JOB_INSTANCE inst on inst.JOB_INSTANCE_ID = ex.JOB_INSTANCE_ID AND inst.JOB_NAME = ? "
            + "ORDER BY ex.JOB_EXECUTION_ID DESC";

    public static final String LAST_SUCCESSFUL_EXECUTION_DATE_BY_SITE_MS = "SELECT TOP 1 ex.START_TIME "
            + "from SpringBatch.BATCH_JOB_EXECUTION ex  "
            + "JOIN SpringBatch.BATCH_JOB_EXECUTION_PARAMS param on param.JOB_EXECUTION_ID = ex.JOB_EXECUTION_ID AND param.KEY_NAME = 'tenantId' and param.LONG_VAL = ? "
            + "JOIN SpringBatch.BATCH_JOB_EXECUTION_PARAMS param2 on param2.JOB_EXECUTION_ID = ex.JOB_EXECUTION_ID AND param2.KEY_NAME = 'siteId' and param2.LONG_VAL = ? "
            + "JOIN SpringBatch.BATCH_JOB_INSTANCE inst on inst.JOB_INSTANCE_ID = ex.JOB_INSTANCE_ID AND inst.JOB_NAME = ? "
            + "ORDER BY ex.JOB_EXECUTION_ID DESC";

    public static final String MULTI_JOB_EXECUTION_BY_TENANT_MS = "SELECT TOP 20 ex.JOB_EXECUTION_ID "
            + "from SpringBatch.BATCH_JOB_EXECUTION ex  "
            + "JOIN SpringBatch.BATCH_JOB_EXECUTION_PARAMS param on param.JOB_EXECUTION_ID = ex.JOB_EXECUTION_ID AND param.KEY_NAME = 'tenantId' and param.LONG_VAL = ? "
            + "JOIN SpringBatch.BATCH_JOB_INSTANCE inst on inst.JOB_INSTANCE_ID = ex.JOB_INSTANCE_ID AND inst.JOB_NAME in (%s) "
            + "ORDER BY ex.JOB_EXECUTION_ID DESC";

    //non sql queries
    public static final String JOB_EXECUTION_BY_TENANT_PG = "SELECT ex.JOB_EXECUTION_ID "
            + "from SpringBatch.BATCH_JOB_EXECUTION ex  "
            + "JOIN SpringBatch.BATCH_JOB_EXECUTION_PARAMS param on param.JOB_EXECUTION_ID = ex.JOB_EXECUTION_ID AND param.KEY_NAME = 'tenantId' and param.LONG_VAL = ? "
            + "JOIN SpringBatch.BATCH_JOB_INSTANCE inst on inst.JOB_INSTANCE_ID = ex.JOB_INSTANCE_ID AND inst.JOB_NAME = ? "
            + "ORDER BY JOB_EXECUTION_ID DESC limit 20";

    public static final String LAST_SUCCESSFUL_EXECUTION_DATE_PG = "SELECT ex.START_TIME "
            + "from SpringBatch.BATCH_JOB_EXECUTION ex  "
            + "JOIN SpringBatch.BATCH_JOB_EXECUTION_PARAMS param on param.JOB_EXECUTION_ID = ex.JOB_EXECUTION_ID AND param.KEY_NAME = 'tenantId' and param.LONG_VAL = ? AND ex.EXIT_CODE = 'COMPLETED'"
            + "JOIN SpringBatch.BATCH_JOB_INSTANCE inst on inst.JOB_INSTANCE_ID = ex.JOB_INSTANCE_ID AND inst.JOB_NAME = ? "
            + "ORDER BY ex.JOB_EXECUTION_ID DESC limit 1";

    public static final String LAST_SUCCESSFUL_EXECUTION_DATE_BY_SITE_PG = "SELECT ex.START_TIME "
            + "from SpringBatch.BATCH_JOB_EXECUTION ex  "
            + "JOIN SpringBatch.BATCH_JOB_EXECUTION_PARAMS param on param.JOB_EXECUTION_ID = ex.JOB_EXECUTION_ID AND param.KEY_NAME = 'tenantId' and param.LONG_VAL = ? AND ex.EXIT_CODE = 'COMPLETED'"
            + "JOIN SpringBatch.BATCH_JOB_EXECUTION_PARAMS param2 on param2.JOB_EXECUTION_ID = ex.JOB_EXECUTION_ID AND param2.KEY_NAME = 'siteId' and param2.LONG_VAL = ? "
            + "JOIN SpringBatch.BATCH_JOB_INSTANCE inst on inst.JOB_INSTANCE_ID = ex.JOB_INSTANCE_ID AND inst.JOB_NAME = ? "
            + "ORDER BY ex.JOB_EXECUTION_ID DESC limit 1";

    public static final String MULTI_JOB_EXECUTION_BY_TENANT_PG = "SELECT ex.JOB_EXECUTION_ID "
            + "from SpringBatch.BATCH_JOB_EXECUTION ex  "
            + "JOIN SpringBatch.BATCH_JOB_EXECUTION_PARAMS param on param.JOB_EXECUTION_ID = ex.JOB_EXECUTION_ID AND param.KEY_NAME = 'tenantId' and param.LONG_VAL = ? "
            + "JOIN SpringBatch.BATCH_JOB_INSTANCE inst on inst.JOB_INSTANCE_ID = ex.JOB_INSTANCE_ID AND inst.JOB_NAME in (%s) "
            + "ORDER BY ex.JOB_EXECUTION_ID DESC limit 20";

    public void setDataSource(DataSource dataSource) {
        setJdbcTemplate(new JdbcTemplate(dataSource));
    }

    public List<Long> getRecentJobExecutionIds(Long tenantId, List<String>jobNames) {
        StringBuilder sb = new StringBuilder();
        for (String jobName : jobNames) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append("'").append(jobName).append("'");
        }
        String statement = isSqlServer() ? MULTI_JOB_EXECUTION_BY_TENANT_MS : MULTI_JOB_EXECUTION_BY_TENANT_PG;
        String query = String.format(statement, sb);
        return getJdbcTemplate().queryForList(query, Long.class, tenantId);
    }

    public List<Long> getRecentJobExecutionIds(Long tenantId, String jobName) {
        String statement = isSqlServer() ? JOB_EXECUTION_BY_TENANT_MS : JOB_EXECUTION_BY_TENANT_PG;
        return getJdbcTemplate().queryForList(statement, Long.class, tenantId, jobName);
    }

    public Timestamp getLastExecutionDate(Long tenantId, String jobName) {
        String statement = isSqlServer() ? LAST_SUCCESSFUL_EXECUTION_DATE_MS : LAST_SUCCESSFUL_EXECUTION_DATE_PG;
        List<Timestamp> lastTimestamps = getJdbcTemplate().queryForList(statement, Timestamp.class, tenantId, jobName);
        Timestamp lastRunTimestamp = null;
        if (!lastTimestamps.isEmpty()) {
            lastRunTimestamp = lastTimestamps.get(0);
        }
        return lastRunTimestamp;
    }

    @Override
    public Timestamp getLastExecutionDate(Long tenantId, Long siteId, String jobName) {
        String statement = isSqlServer() ? LAST_SUCCESSFUL_EXECUTION_DATE_BY_SITE_MS : LAST_SUCCESSFUL_EXECUTION_DATE_BY_SITE_PG;
        List<Timestamp> lastTimestamps = getJdbcTemplate().queryForList(statement, Timestamp.class, tenantId, siteId, jobName);
        Timestamp lastRunTimestamp = null;
        if (!lastTimestamps.isEmpty()) {
            lastRunTimestamp = lastTimestamps.get(0);
        }
        return lastRunTimestamp;
    }

    private boolean isSqlServer() {
        return Boolean.TRUE.equals(isSql);
    }

    public synchronized void init() {
        if (isSql == null) {
            JdbcTemplate jdbcTemplate = (JdbcTemplate) getJdbcTemplate();
            String dbProductName = null;
            try (Connection connection = jdbcTemplate.getDataSource().getConnection()) {
                dbProductName = connection.getMetaData().getDatabaseProductName();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            isSql = dbProductName != null && dbProductName.toLowerCase().contains("microsoft");
        }
    }

}
