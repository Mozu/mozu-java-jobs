package com.mozu.jobs.jdbc.datasource;

import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.mozu.encryptor.PropertyEncryptionUtil;

public class MozuDriverManagerDataSource extends DriverManagerDataSource {
    protected String spiceKey = null;
    
    @Override
    public String getPassword() {
        return PropertyEncryptionUtil.decryptProperty(getSpiceKey(), super.getPassword());
    }

    public String getSpiceKey() {
        return spiceKey;
    }

    public void setSpiceKey(String spiceKey) {
        this.spiceKey = spiceKey;
    }
    
    
}
