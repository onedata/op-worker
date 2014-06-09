// ===================================================================
// @author Krzysztof Trzepla
// @copyright (C): 2014 ACK CYFRONET AGH
// This software is released under the MIT license
// cited in 'LICENSE.txt'.
// @end
// ===================================================================
// @doc: This file contains library functions that renders charts on
// monitoring web page using Google Charts API
// @end
// ===================================================================

// global map that holds all available on page charts
var charts = {};

var months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

// adds leading zero to digit
function formatNumber(number) {
    if (number < 10) {
        return "0" + number;
    } else {
        return number;
    }
}

// changes timestamp from second format to hh:mm:ss \n MM:DD:YYYY
function formatTimestamp(timestamp) {
    var date = new Date(parseInt(timestamp));
    return "\n" + formatNumber(date.getHours()) + ":" + formatNumber(date.getMinutes()) + ":" + formatNumber(date.getSeconds()) + "\n" +
        months[date.getMonth()] + " " + date.getDate() + ", " + date.getFullYear() + "\n";
}

// creates and renders new chart object on page, this object is stored in charts map using given id
function createChart(id, type, title, vAxisTitle, header, body) {
    var chart = {};

    if (type == "LineChart") {
        chart.self = new google.visualization.LineChart(document.getElementById('chart_' + id));
    } else if (type == "AreaChart") {
        chart.self = new google.visualization.AreaChart(document.getElementById('chart_' + id));
    } else {
        return 0;
    }

    chart.dataTable = new google.visualization.DataTable(header);

    chart.dataTable.addRows(body);

    for (var row = 0; row < chart.dataTable.getNumberOfRows(); row++) {
        var timestamp = chart.dataTable.getValue(row, 0);
        chart.dataTable.setValue(row, 0, formatTimestamp(timestamp));
    }

    chart.options = {
        title: title,
        height: 400,
        legend: 'right',
        hAxis: {
            title: 'Time',
            showTextEvery: parseInt(chart.dataTable.getNumberOfRows() / 5)
        },
        vAxis: {
            title: vAxisTitle,
            viewWindow: {
                min: 0
            }
        }
    };

    if (type == "LineChart") {
        chart.options.curveType = 'function'
    }

    chart.self.draw(chart.dataTable, chart.options);

    charts[id] = chart;

    return !0;
}

// updates chart object in charts map and renders new object on page
function updateChart(id, rows) {
    var chart = charts[id];

    for (var row = 0; row < rows.length; row++) {
        rows[row][0] = formatTimestamp(rows[row][0]);
    }

    chart.dataTable.removeRows(0, chart.dataTable.getNumberOfRows());
    chart.dataTable.addRows(rows);

    chart.self.draw(chart.dataTable, chart.options);
}

// deletes object from chart map
function deleteChart(id) {
    var chart = charts[id];

    chart.self.clearChart();
    chart.dataTable = null;
    chart.options = null;

    delete charts[id];

    document.getElementById('row_' + id).remove();
}
