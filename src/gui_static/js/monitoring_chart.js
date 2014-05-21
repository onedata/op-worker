function drawChart(jsonData, jsonID, jsonChart, jsonTitle) {

    var dataTable = new google.visualization.DataTable(jsonData);

    var dateFormatter = new google.visualization.DateFormat({pattern: 'MMM dd, yyyy\nHH:mm:ss'});
    dateFormatter.format(dataTable, 0);

    var columns = [
        {calc: function (data, row) {
            return data.getFormattedValue(row, 0);
        }, type: 'string'}
    ];
    var title;
    var legend;
    var vAxisTitle;
    var chart;

    var numberOfColumns = dataTable.getNumberOfColumns();
    for (var i = 1; i < numberOfColumns; i++) {
        var label = dataTable.getColumnLabel(i);
        switch (jsonChart) {
            case "CPU utilization":
                chart = new google.visualization.LineChart(document.getElementById('chart_' + jsonID));
                if (label == "cpu") {
                    columns.push(i);
                    dataTable.setColumnLabel(i, "CPU");
                    title = "CPU Utilization" + jsonTitle;
                    legend = "none";
                    vAxisTitle = "Utilization [%]";
                } else if (label.indexOf("core") == 0) {
                    columns.push(i);
                    dataTable.setColumnLabel(i, "Core" + label.substring(4));
                    title = "CPU Load" + jsonTitle;
                    legend = "none";
                    vAxisTitle = "Load [%]";
                }
                break;
            case "memory usage":
                chart = new google.visualization.LineChart(document.getElementById('chart_' + jsonID));
                if (label == "mem") {
                    columns.push(i);
                    dataTable.setColumnLabel(i, "Memory");
                    title = "Memory usage" + jsonTitle;
                    legend = "none";
                    vAxisTitle = "Usage [%]";
                }
                break;
            case "network throughput":
                chart = new google.visualization.AreaChart(document.getElementById('chart_' + jsonID));
                if (label == "net_rx_pps") {
                    columns.push(i);
                    dataTable.setColumnLabel(i, "RX pps");
                    title = "Network throughput" + jsonTitle;
                    legend = "right";
                    vAxisTitle = "packets / sec";
                } else if (label.indexOf("net_rx_pps_") == 0) {
                    columns.push(i);
                    dataTable.setColumnLabel(i, label.substring(11) + " RX pps");
                    title = "Network throughput" + jsonTitle;
                    legend = "right";
                    vAxisTitle = "packets / sec";
                }
                if (label == "net_tx_pps") {
                    columns.push(i);
                    dataTable.setColumnLabel(i, "TX pps");
                    title = "Network throughput" + jsonTitle;
                    legend = "right";
                    vAxisTitle = "packets / sec";
                } else if (label.indexOf("net_tx_pps_") == 0) {
                    columns.push(i);
                    dataTable.setColumnLabel(i, label.substring(11) + " TX pps");
                    title = "Network throughput" + jsonTitle;
                    legend = "right";
                    vAxisTitle = "packets / sec";
                }
                break;
            case "network transfer":
                chart = new google.visualization.AreaChart(document.getElementById('chart_' + jsonID));
                if (label == "net_rx_b") {
                    columns.push(i);
                    dataTable.setColumnLabel(i, "RX bytes");
                    title = "Network transfer" + jsonTitle;
                    legend = "right";
                    vAxisTitle = "bytes";
                } else if (label.indexOf("net_rx_b_") == 0) {
                    columns.push(i);
                    dataTable.setColumnLabel(i, label.substring(9) + " RX bytes");
                    title = "Network transfer" + jsonTitle;
                    legend = "right";
                    vAxisTitle = "bytes";
                }
                if (label == "net_tx_b") {
                    columns.push(i);
                    dataTable.setColumnLabel(i, "TX bytes");
                    title = "Network transfer" + jsonTitle;
                    legend = "right";
                    vAxisTitle = "bytes";
                } else if (label.indexOf("net_tx_b_") == 0) {
                    columns.push(i);
                    dataTable.setColumnLabel(i, label.substring(9) + " TX bytes");
                    title = "Network transfer" + jsonTitle;
                    legend = "right";
                    vAxisTitle = "bytes";
                }
                break;
            case "ports transfer":
                chart = new google.visualization.AreaChart(document.getElementById('chart_' + jsonID));
                if (label == "ports_rx_b") {
                    columns.push(i);
                    dataTable.setColumnLabel(i, "RX bytes");
                    title = "Ports transfer" + jsonTitle;
                    legend = "right";
                    vAxisTitle = "bytes";
                }
                if (label == "ports_tx_b") {
                    columns.push(i);
                    dataTable.setColumnLabel(i, "TX bytes");
                    title = "Ports transfer" + jsonTitle;
                    legend = "right";
                    vAxisTitle = "bytes";
                }
                break;
            case "storage IO transfer":
                chart = new google.visualization.AreaChart(document.getElementById('chart_' + jsonID));
                if (label == "storage_read_b") {
                    columns.push(i);
                    dataTable.setColumnLabel(i, "read bytes");
                    title = "Storage transfer" + jsonTitle;
                    legend = "right";
                    vAxisTitle = "bytes";
                }
                if (label == "storage_write_b") {
                    columns.push(i);
                    dataTable.setColumnLabel(i, "write bytes");
                    title = "Storage transfer" + jsonTitle;
                    legend = "right";
                    vAxisTitle = "bytes";
                }
                break;
            default:
                return !0;
        }
    }

    var dataView = new google.visualization.DataView(dataTable);

    dataView.setColumns(columns);

    var showEvery = parseInt(dataTable.getNumberOfRows() / 5);

    var options = {
        title: title,
        height: 300,
        legend: legend,
        hAxis: {
            title: 'Time',
            showTextEvery: showEvery
        },
        vAxis: {
            title: vAxisTitle
        }
    };

    chart.draw(dataView, options);
    return !0;
}