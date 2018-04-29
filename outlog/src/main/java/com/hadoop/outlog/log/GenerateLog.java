package com.hadoop.outlog.log;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Date;


/**
 * @author jianz
 */
public class GenerateLog {
	public static void generLog() throws Exception {
		Logger logger = LogManager.getLogger("testlog");
		int i = 0;
		while (true) {
			logger.info(new Date().toString() + "-----------------------------");
			i++;
			Thread.sleep(500);
			if (i > 1000000)
				break;

		}
	}

}
