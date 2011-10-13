package com.nexr.platform.search.result.utils;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.ApplicationFrame;

import java.awt.*;
import java.io.File;
import java.io.IOException;

/**
 * User: david
 * Date: 7/18/11
 * Time: 5:02 PM
 */
public class Chart extends ApplicationFrame {

    private JFreeChart _chart;

    public Chart(XYSeriesCollection dataSet, String title, int serverCount) throws IOException {
        super(title);
        _chart = createChart(dataSet, title, serverCount);
    }

    public void save(String saveFilePath, int width, int height) throws IOException {
        ChartUtilities.saveChartAsPNG(new File(saveFilePath), _chart, width, height);
    }

    public static XYSeriesCollection createDataSet() {

        final XYSeries series = new XYSeries("Random Data");

        series.add(1.0, 500.2);
        series.add(5.0, 694.1);
        series.add(4.0, 100.0);
        series.add(12.5, 734.4);
        series.add(17.3, 453.2);
        series.add(21.2, 500.2);
        series.add(21.9, 10.0);
        series.add(25.6, 734.4);
        series.add(30.0, 453.2);

        final XYSeries series2 = new XYSeries("Random Data2");

        series2.add(1.0, 13.2);
        series2.add(5.0, 50.4);
        series2.add(4.0, 8.1);
        series2.add(12.5, 200.3);
        series2.add(17.3, 573.2);
        series2.add(21.2, 831.2);
        series2.add(21.9, 492.1);
        series2.add(25.6, 1111.2);
        series2.add(30.0, 0.0);

        final XYSeriesCollection data = new XYSeriesCollection();
        data.addSeries(series);
        data.addSeries(series2);

        return data;

    }

    private JFreeChart createChart(final XYSeriesCollection dataSet, String title, int serverCount) {

        final JFreeChart chart = ChartFactory.createXYLineChart(
            title,                     // chart title
            "run count",          // domain axis label
            "ns (nano seconds)",                     // range axis label
            dataSet,                   // data
            PlotOrientation.VERTICAL,  // orientation
            true,                      // include legend
            true,                      // tooltips
            false                      // urls
        );

        XYPlot plot = (XYPlot) chart.getPlot();
        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();

        plot.setBackgroundPaint(Color.white);
        plot.setRangeGridlinePaint(Color.lightGray);

        for(int i = 0 ; i < serverCount; i++){
            renderer.setSeriesLinesVisible(i, true);
            renderer.setSeriesShapesVisible(i, false);

            renderer.setSeriesStroke(
            i, new BasicStroke(
                    1.5f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND,
                    0.5f, new float[] {7.5f, 4.0f}, 0.0f
                )
            );
        }


        plot.setRenderer(renderer);

        /*chart.setBackgroundPaint(Color.white);

        final CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setBackgroundPaint(Color.white);
        plot.setRangeGridlinePaint(Color.lightGray);
        plot.setAnchorValue(1000);

        CategoryAxis domainAxis = (CategoryAxis) plot.getDomainAxis();
        // domainAxis.setFixedDimension(3600.0);
        // domainAxis.setCategoryMargin(1.0);

        final NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
        rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
        rangeAxis.setAutoRangeIncludesZero(true);

        final LineAndShapeRenderer renderer = (LineAndShapeRenderer) plot.getRenderer();

        for(int i = 0 ; i < serverCount; i++){
            renderer.setSeriesStroke(
            i, new BasicStroke(
                    2.0f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND,
                    1.0f, new float[] {10.0f, 6.0f}, 0.0f
                )
            );
        }*/

        return chart;
    }

    public static void main(final String[] args) {

        try {
            XYSeriesCollection dataSet = Chart.createDataSet();
            Chart chart = new Chart(dataSet, "Line Chart Demo", dataSet.getSeriesCount());
            chart.save("/home/david/1.png", 600, 500);

        } catch (Exception e){
            e.printStackTrace();
        }
    }

}
