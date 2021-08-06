package com.ct.analysis;


import com.ct.analysis.tool.AnalysisTextTool;
import org.apache.hadoop.util.ToolRunner;

public class AnalysisData {

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new AnalysisTextTool(), args);

    }
}
