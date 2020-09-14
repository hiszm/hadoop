package com.bigdata.hadoop.hdfs;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        Mapper mapper= new Mapper();
        Reducer reducer=new Reducer();

        assertTrue( true );
    }


}
