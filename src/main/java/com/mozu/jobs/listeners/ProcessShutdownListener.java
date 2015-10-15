package com.mozu.jobs.listeners;

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.launch.JobExecutionNotRunningException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

public class ProcessShutdownListener implements JobExecutionListener {
   
	private static final Logger logger = LoggerFactory.getLogger(ProcessShutdownListener.class);
   
	private JobOperator jobOperator;
     
    @Override
    public void afterJob(JobExecution jobExecution) {  }
 
    @Override
    public void beforeJob(final JobExecution jobExecution) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
            	super.run();
                try {
                	Enumeration<Driver> drivers = DriverManager.getDrivers();
                	if(drivers.hasMoreElements()){
	                	jobOperator.stop(jobExecution.getId());
	                    while(jobExecution.isRunning()) {
	                        logger.info("waiting for job to stop...");
	                        try {Thread.sleep(100);} 
	                        catch (InterruptedException e) {
	                        	logger.warn(e.getMessage(), e);
	                        }
	                    }
                	}
                } catch (NoSuchJobExecutionException e) { 
                	logger.warn(e.getMessage(), e);
                } catch (JobExecutionNotRunningException e) { 
                	logger.warn(e.getMessage(), e);
                }
            }
        });
    }
 
    public void setJobOperator(JobOperator jobOperator) {
        this.jobOperator = jobOperator;
    }

	
 
}