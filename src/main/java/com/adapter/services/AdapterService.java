package com.adapter.services;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.ExecutionException;

import org.jrobin.core.RrdException;

public interface AdapterService {


	int FetchRRDData(long startDate, long endDate, int poolSize) throws IOException, SQLException, RrdException, InterruptedException, ExecutionException;

}
