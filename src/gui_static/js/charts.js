var charts = {};

var months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

function formatNumber(number) {
    if (number < 10) {
        return "0" + number;
    } else {
        return number;
    }
}

function formatTimestamp(timestamp) {
    var date = new Date(parseInt(timestamp));
    return "\n" + formatNumber(date.getHours()) + ":" + formatNumber(date.getMinutes()) + ":" + formatNumber(date.getSeconds()) + "\n" +
        months[date.getMonth()] + " " + date.getDate() + ", " + date.getFullYear() + "\n";
}

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

function updateChart(id, rows) {
    var chart = charts[id];

    for (var row = 0; row < rows.length; row++) {
        rows[row][0] = formatTimestamp(rows[row][0]);
    }

    chart.dataTable.removeRows(0, chart.dataTable.getNumberOfRows());
    chart.dataTable.addRows(rows);

    chart.self.draw(chart.dataTable, chart.options);
}

function deleteChart(id) {
    var chart = charts[id];

    chart.self.clearChart();
    chart.dataTable = null;
    chart.options = null;

    delete charts[id];

    document.getElementById('row_' + id).remove();
}
