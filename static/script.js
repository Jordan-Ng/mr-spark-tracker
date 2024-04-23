window.onload = () => {

    const api_base_url = document.head.querySelector("[property~=data][content]").content
    
    // global variables (refactor me!)
    let mapred = {
        chartData : [],
        tableData : []
    }

    let spark = {
        chartData : [],
        tableData : []
    }

    const job_mapper = {
        "mapred" : mapred,
        "spark" : spark
    }

    // helper functions
    const parseRawCSV = (job, data) => {
        const entries = data.split("\r\n")
        
        for (let i=1; i < entries.length; i++){
            if (entries[i] == "") continue
            const entry = entries[i].split(",")
            
            job.tableData.push(entry)      // whole data point               
            job.chartData.push(entry[2])   // elapsed time   
        }

        return true
    }

    const buildChart = (target) => {
        const ctx = document.getElementById(target)
        
        new Chart(ctx, {
                type: 'bar',
            data: {
              labels: [...Array(mapred.chartData.length).keys()].map(x => x+1),
              datasets: [{
                label: 'MapReduce',
                data: mapred.chartData,
                borderWidth: 1
              },
              {
                label: 'Spark',
                data: spark.chartData,
                borderWidth: 1
              }
            ]
            },
            options: {
              scales: {
                y: {
                  beginAtZero: true,
                  title: {
                    display: true,
                    text: "elapsed time (ms)"
                  }
                },
                x: {
                    title: {
                      display: true,
                      text: "Job ID"
                    }
                }
              }
            }
            })
    }

    const buildTable = (target) => {
        const targetElement = document.getElementById(target)
        const job = target.split("_")[0]
        let tableStructure = `
            <table id=${job}>
                <tr>
                    <th>Start Timestamp</th>
                    <th>End Timestamp</th>
                    <th>Elapsed Time (ms)</th>
                </tr>                        
                ${job_mapper[job].tableData.map(entry =>
                        `<tr>
                            <td>${entry[0]}</td>
                            <td>${entry[1]}</td>
                            <td>${entry[2]}</td>
                        </tr>`
                ).join('')}
            </table>
        `
        
        targetElement.innerHTML = tableStructure
    }
    
    const appendRow = (target, data) => {
        const table = document.getElementById(target)
        let row = table.insertRow(-1)
        let c1 = row.insertCell(0)
        let c2 = row.insertCell(1)
        let c3 = row.insertCell(2)
        c1.innerText = data[0]
        c2.innerText = data[1]
        c3.innerText = data[2]
    }

    // establish websocket connection
    const ws = new WebSocket(`ws://${api_base_url}/ws`)

    // websocket broadcast event listener
    ws.onmessage = function(event) {
        const message = JSON.parse(event.data)

        if (message.type == "data"){
            // appendRow("mapred", message.data)
            appendRow(message.job, message.data)
            job_mapper[message.job].chartData.push(message.data[2])

            chartStatus = Chart.getChart("chart")
            if ( chartStatus != undefined){
                chartStatus.destroy()
                buildChart("chart")
            }
            
        }
        else {
            const toast_container = document.getElementById(`${message.job}_toast`)
            toast_container.innerText = message.message
        }
    }

    // ----- Driver -----
    // retrieve csv files + build table and chart
    Promise.all([

    fetch(`http://${api_base_url}/static/mapred.csv`,{method: "GET"})
    .then(data => data.text())
    .then(dataText => {
            parseRawCSV(mapred, dataText)
            buildTable('mapred_table')})
    .catch(error => console.log(error))
    ,
    fetch(`http://${api_base_url}/static/spark.csv`, {method: "GET"})
    .then(data => data.text())
    .then(dataText => {
            parseRawCSV(spark, dataText)
            buildTable("spark_table")})
    .catch(error => console.log(err))
]

    ).then(() => buildChart("chart"))
}