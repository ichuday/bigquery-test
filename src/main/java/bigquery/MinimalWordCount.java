/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bigquery;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.BeamRecordSqlType;
import org.apache.beam.sdk.extensions.sql.BeamSql;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.PCollection;
import com.google.api.services.bigquery.model.TableRow;

public class MinimalWordCount {

	public static void main(String[] args) {

		DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
		options.setRunner(DataflowRunner.class);
		options.setProject("ad-efficiency-dev");
		options.setStagingLocation("gs://adefficiency/staging");
		DataflowRunner.fromOptions(options);

		Pipeline p = Pipeline.create(options);

		PCollection<TableRow> qData = p
				.apply(BigQueryIO.readTableRows().fromQuery("SELECT * FROM [Ad_Efficiency.Composite_Map]"));
		List<String> fieldNames13 = Arrays.asList("Level", "Country", "PeriodType", "ReportPeriod", "ActualPeriod",
				"SourceBDA", "BrandChapter", "Studio",

				"Neighborhoods", "BU", "Division", "SubChannel", "MediaChannel", "Market", "ConsumerBehaviour",
				"EventName", "Published", "Spend", "GRP", "Duration",

				"Continuity", "Basis", "BasisDur", "Alpha", "Beta", "Typical", "Beneficiary", "Catlib", "DueToVol",
				"DartSpendActual", "PlannedSpendMediaTools",

				"AdstockRatio", "Channel", "ChVol", "AOVol", "Proj", "rNR", "rNCS", "rCtb", "rAC", "rCtbNorm",
				"rACNorm", "V", "CPP",

				"NCS", "Ctb", "AC", "NCS1", "Ctb1", "AC1", "Cont", "xMarginal", "vNorm", "xNorm", "xNormOld");

		List<Integer> fieldTypes13 = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
				Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
				Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,

				Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
				Types.DOUBLE, Types.DOUBLE, Types.VARCHAR, Types.VARCHAR, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
				Types.DOUBLE, Types.VARCHAR, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
				Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,

				Types.VARCHAR, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
				Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE);

		
		final BeamRecordSqlType appType13 = BeamRecordSqlType.create(fieldNames13, fieldTypes13);
		
		 PCollection<BeamRecord> apps13 = qData.apply(ParDo.of(new DoFn<TableRow, BeamRecord>() {

             private static final long serialVersionUID = 1L;

             @ProcessElement

             public void processElement(ProcessContext c) {
            	 TableRow row = c.element();
            	 String Level = (String) row.get("Level");
            	 String Country = (String) row.get("Country");
            	 String PeriodType = (String) row.get("Period_Type");
            	 String ReportPeriod = (String) row.get("Report_Period");
            	 String ActualPeriod = (String) row.get("Actual_Period");
            	 String SourceBDA = (String) row.get("Source_BDA");
            	 String BrandChapter = (String) row.get("Brand_Chapter");
            	 String Studio = (String) row.get("Studio");
            	 String Neighborhoods = (String) row.get("Neighborhoods");
            	 String BU = (String) row.get("BU");
            	 String Division = (String) row.get("Division");
            	 String SubChannel = (String) row.get("Sub_channel");
            	 String MediaChannel = (String) row.get("Media_Channel");
            	 String Market = (String) row.get("Market");
            	 String ConsumerBehaviour = (String) row.get("Consumer_Behaviour");
            	 String EventName = (String) row.get("Event_Name");
            	 String Published = (String) row.get("Published");
            	 Double Spend = (Double) row.get("Spend");
            	 Double GRP = (Double) row.get("GRP");
            	 Double Duration = (Double) row.get("Duration");
            	 Double Continuity = (Double) row.get("Continuity");
            	 Double Basis = (Double) row.get("Basis");
            	 Double BasisDur = (Double) row.get("BasisDur");
            	 Double Alpha = (Double) row.get("Alpha");
            	 Double Beta = (Double) row.get("Beta");
            	 Double Typical = (Double) row.get("Typical");
            	 String Beneficiary = (String) row.get("Beneficiary");
            	 String Catlib = (String) row.get("Catlib");
            	 Double DueToVol = (Double) row.get("DueToVol");
            	 Double DartSpendActual = (Double) row.get("Dart_Spend_Actual");
            	 Double PlannedSpendMediaTools = (Double) row.get("Planned_Spend_Media_Tools");
            	 Double AdstockRatio = (Double) row.get("AdstockRatio");
            	 String Channel = (String) row.get("Channel");
            	 Double ChVol = (Double) row.get("ChVol");
            	 Double AOVol = (Double) row.get("AOVol");
            	 Double Proj = (Double) row.get("Proj");
            	 Double rNR = (Double) row.get("rNR");
            	 Double rNCS = (Double) row.get("rNCS");
            	 Double rCtb = (Double) row.get("rCtb");
            	 Double rAC = (Double) row.get("rAC");
            	 Double rCtbNorm = (Double) row.get("rCtbNorm");
            	 Double rACNorm = (Double) row.get("rACNorm");
            	 Double V = (Double) row.get("V");
            	 String CPP = (String) row.get("CPP");
            	 Double NCS = (Double) row.get("NCS");
            	 Double Ctb = (Double) row.get("Ctb");
            	 Double AC = (Double) row.get("AC");
            	 Double NCS1 = (Double) row.get("NCS1");
            	 Double Ctb1 = (Double) row.get("Ctb1");
            	 Double AC1 = (Double) row.get("AC1");
            	 Double Cont = (Double) row.get("Cont");
            	 Double xMarginal = (Double) row.get("xMarginal");
            	 Double vNorm = (Double) row.get("vNorm");
            	 Double xNorm = (Double) row.get("xNorm");
            	 Double xNormOld = (Double) row.get("xNormOld");
            
            	 
                    BeamRecord br = new BeamRecord(appType13, Level,Country, PeriodType,ReportPeriod, ActualPeriod,SourceBDA,BrandChapter,Studio,

                                  Neighborhoods, BU, Division, SubChannel, MediaChannel,Market,ConsumerBehaviour, EventName,Published,Spend,GRP, Duration,

                                  Continuity,Basis, BasisDur,Alpha, Beta, Typical, Beneficiary, Catlib, DueToVol, DartSpendActual, PlannedSpendMediaTools,

                                 AdstockRatio, Channel, ChVol,AOVol, Proj, rNR, rNCS, rCtb, rAC, rCtbNorm, rACNorm, V, CPP,

                                  NCS, Ctb, AC,NCS1,Ctb1,AC1, Cont, xMarginal, vNorm, xNorm, xNormOld);

                    c.output(br); 
                    }}));
		
		PCollection<BeamRecord> record7 = apps13.apply(BeamSql.query(" SELECT * from PCOLLECTION"));

		p.run().waitUntilFinish();
	}

}
