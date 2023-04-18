/*
 * COPYRIGHT (C) 2014 Volusion Inc. All Rights Reserved.
 */
package com.mozu.jobs.dao;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.batch.core.repository.dao.AbstractJdbcBatchMetadataDao;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;

public class JobExecutionDaoImpl extends AbstractJdbcBatchMetadataDao implements JobExecutionDao {
    private DataSource dataSource;
    private Boolean isSql ;

    //sql queries
    public final String JOB_EXECUTION_BY_TENANT_MS = "SELECT TOP 20 ex.JOB_EXECUTION_ID "
                                                    + "from SpringBatch.BATCH_JOB_EXECUTION ex  "
                                                    + "JOIN SpringBatch.BATCH_JOB_EXECUTION_PARAMS param on param.JOB_EXECUTION_ID = ex.JOB_EXECUTION_ID AND param.KEY_NAME = 'tenantId' and param.LONG_VAL = ? "
                                                    + "JOIN SpringBatch.BATCH_JOB_INSTANCE inst on inst.JOB_INSTANCE_ID = ex.JOB_INSTANCE_ID AND inst.JOB_NAME = ? "
                                                    + "ORDER BY JOB_EXECUTION_ID DESC";

    public final String LAST_SUCCESSFUL_EXECUTION_DATE_MS = "SELECT TOP 1 ex.START_TIME "
            + "from SpringBatch.BATCH_JOB_EXECUTION ex  "
            + "JOIN SpringBatch.BATCH_JOB_EXECUTION_PARAMS param on param.JOB_EXECUTION_ID = ex.JOB_EXECUTION_ID AND param.KEY_NAME = 'tenantId' and param.LONG_VAL = ? "
            + "JOIN SpringBatch.BATCH_JOB_INSTANCE inst on inst.JOB_INSTANCE_ID = ex.JOB_INSTANCE_ID AND inst.JOB_NAME = ? "
            + "ORDER BY ex.JOB_EXECUTION_ID DESC";

    public final String LAST_SUCCESSFUL_EXECUTION_DATE_BY_SITE_MS = "SELECT TOP 1 ex.START_TIME "
            + "from SpringBatch.BATCH_JOB_EXECUTION ex  "
            + "JOIN SpringBatch.BATCH_JOB_EXECUTION_PARAMS param on param.JOB_EXECUTION_ID = ex.JOB_EXECUTION_ID AND param.KEY_NAME = 'tenantId' and param.LONG_VAL = ? "
            + "JOIN SpringBatch.BATCH_JOB_EXECUTION_PARAMS param2 on param2.JOB_EXECUTION_ID = ex.JOB_EXECUTION_ID AND param2.KEY_NAME = 'siteId' and param2.LONG_VAL = ? "
            + "JOIN SpringBatch.BATCH_JOB_INSTANCE inst on inst.JOB_INSTANCE_ID = ex.JOB_INSTANCE_ID AND inst.JOB_NAME = ? "
            + "ORDER BY ex.JOB_EXECUTION_ID DESC";



    public final String MULTI_JOB_EXECUTION_BY_TENANT_MS = "SELECT TOP 20 ex.JOB_EXECUTION_ID "
            + "from SpringBatch.BATCH_JOB_EXECUTION ex  "
            + "JOIN SpringBatch.BATCH_JOB_EXECUTION_PARAMS param on param.JOB_EXECUTION_ID = ex.JOB_EXECUTION_ID AND param.KEY_NAME = 'tenantId' and param.LONG_VAL = ? "
            + "JOIN SpringBatch.BATCH_JOB_INSTANCE inst on inst.JOB_INSTANCE_ID = ex.JOB_INSTANCE_ID AND inst.JOB_NAME in (%s) "
            + "ORDER BY ex.JOB_EXECUTION_ID DESC";
    //non sql queries

    public final String JOB_EXECUTION_BY_TENANT_PG = "SELECT ex.JOB_EXECUTION_ID "
            + "from SpringBatch.BATCH_JOB_EXECUTION ex  "
            + "JOIN SpringBatch.BATCH_JOB_EXECUTION_PARAMS param on param.JOB_EXECUTION_ID = ex.JOB_EXECUTION_ID AND param.KEY_NAME = 'tenantId' and param.LONG_VAL = ? "
            + "JOIN SpringBatch.BATCH_JOB_INSTANCE inst on inst.JOB_INSTANCE_ID = ex.JOB_INSTANCE_ID AND inst.JOB_NAME = ? "
            + "ORDER BY JOB_EXECUTION_ID DESC limit 20";

    public final String LAST_SUCCESSFUL_EXECUTION_DATE_PG = "SELECT ex.START_TIME "
            + "from SpringBatch.BATCH_JOB_EXECUTION ex  "
            + "JOIN SpringBatch.BATCH_JOB_EXECUTION_PARAMS param on param.JOB_EXECUTION_ID = ex.JOB_EXECUTION_ID AND param.KEY_NAME = 'tenantId' and param.LONG_VAL = ? AND ex.EXIT_CODE = 'COMPLETED'"
            + "JOIN SpringBatch.BATCH_JOB_INSTANCE inst on inst.JOB_INSTANCE_ID = ex.JOB_INSTANCE_ID AND inst.JOB_NAME = ? "
            + "ORDER BY ex.JOB_EXECUTION_ID DESC limit 1";

    public final String LAST_SUCCESSFUL_EXECUTION_DATE_BY_SITE_PG = "SELECT ex.START_TIME "
            + "from SpringBatch.BATCH_JOB_EXECUTION ex  "
            + "JOIN SpringBatch.BATCH_JOB_EXECUTION_PARAMS param on param.JOB_EXECUTION_ID = ex.JOB_EXECUTION_ID AND param.KEY_NAME = 'tenantId' and param.LONG_VAL = ? AND ex.EXIT_CODE = 'COMPLETED'"
            + "JOIN SpringBatch.BATCH_JOB_EXECUTION_PARAMS param2 on param2.JOB_EXECUTION_ID = ex.JOB_EXECUTION_ID AND param2.KEY_NAME = 'siteId' and param2.LONG_VAL = ? "
            + "JOIN SpringBatch.BATCH_JOB_INSTANCE inst on inst.JOB_INSTANCE_ID = ex.JOB_INSTANCE_ID AND inst.JOB_NAME = ? "
            + "ORDER BY ex.JOB_EXECUTION_ID DESC limit 1";

    public final String MULTI_JOB_EXECUTION_BY_TENANT_PG = "SELECT ex.JOB_EXECUTION_ID "
            + "from SpringBatch.BATCH_JOB_EXECUTION ex  "
            + "JOIN SpringBatch.BATCH_JOB_EXECUTION_PARAMS param on param.JOB_EXECUTION_ID = ex.JOB_EXECUTION_ID AND param.KEY_NAME = 'tenantId' and param.LONG_VAL = ? "
            + "JOIN SpringBatch.BATCH_JOB_INSTANCE inst on inst.JOB_INSTANCE_ID = ex.JOB_INSTANCE_ID AND inst.JOB_NAME in (%s) "
            + "ORDER BY ex.JOB_EXECUTION_ID DESC limit 20";


    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        setJdbcTemplate(new JdbcTemplate(dataSource));
    }

    public List<Long> getRecentJobExecutionIds(Long tenantId, List<String>jobNames) {
        StringBuffer jobStringBuffer = new StringBuffer();
        for (String jobName : jobNames) {
            if (jobStringBuffer.length() > 0) {
                jobStringBuffer.append(",");
            }
            jobStringBuffer.append("'").append(jobName).append("'");
        }
        String statement = isSqlServer(getJdbcTemplate()) ? MULTI_JOB_EXECUTION_BY_TENANT_MS : MULTI_JOB_EXECUTION_BY_TENANT_PG;
        String query = String.format(statement, jobStringBuffer.toString());
        return getJdbcTemplate().queryForList(query, Long.class, tenantId);
    }

    public List<Long> getRecentJobExecutionIds(Long tenantId, String jobName) {
        String statement = isSqlServer(getJdbcTemplate()) ? JOB_EXECUTION_BY_TENANT_MS : JOB_EXECUTION_BY_TENANT_PG;

        return getJdbcTemplate().queryForList(statement, Long.class, tenantId, jobName);
    }

    public Timestamp getLastExecutionDate(Long tenantId, String jobName) {
        String statement = isSqlServer(getJdbcTemplate()) ? LAST_SUCCESSFUL_EXECUTION_DATE_MS : LAST_SUCCESSFUL_EXECUTION_DATE_PG;
        List<Timestamp> lastTimestamps = getJdbcTemplate().queryForList(statement, Timestamp.class, tenantId, jobName);
        Timestamp lastRunTimestamp = null;
        if (lastTimestamps.size() > 0) {
            lastRunTimestamp = lastTimestamps.get(0);
        }
        return lastRunTimestamp;
    }

    @Override
    public Timestamp getLastExecutionDate(Long tenantId, Long siteId, String jobName) {

        String statement = isSqlServer(getJdbcTemplate()) ? LAST_SUCCESSFUL_EXECUTION_DATE_BY_SITE_MS : LAST_SUCCESSFUL_EXECUTION_DATE_BY_SITE_PG;
        JdbcOperations jobOperation  = getJdbcTemplate();
        List<Timestamp> lastTimestamps = getJdbcTemplate().queryForList(statement, Timestamp.class, tenantId, siteId, jobName);
        Timestamp lastRunTimestamp = null;
        if (lastTimestamps.size() > 0) {
            lastRunTimestamp = lastTimestamps.get(0);
        }
        return lastRunTimestamp;
    }

    private boolean isSqlServer (JdbcOperations jdbcOperations) {
        if ( isSql == null) {
            JdbcTemplate jdbcTemplate = (JdbcTemplate) jdbcOperations;
            String dbProductName = null;
            try {
                DatabaseMetaData metaData = jdbcTemplate.getDataSource().getConnection().getMetaData();
                dbProductName = metaData.getDatabaseProductName();
            } catch (SQLException e) {
                e.printStackTrace();
            }

            if (dbProductName != null) {
                isSql = dbProductName.toLowerCase().contains("microsoft");
            }else{
                isSql = true;
            }
        }
        return isSql;
    }
}
