window.onload = () => {
    // global variables
    let mapred = {
        chartData : [],
        tableData : []
    }

    // helper functions
    const parseRawCSV = (data) => {
        const entries = data.split("\r\n")
        // console.log(entries)
        for (let i=1; i < entries.length; i++){
            if (entries[i] == "") continue
            const entry = entries[i].split(",")
            mapred.tableData.push(entry)      // whole data point
            mapred.chartData.push(entry[2])   // elapsed time   
        }

        return true
    }

    const buildChart = (target) => {
        const ctx = document.getElementById(target)
        // console.log(mapred.tableData)
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
                data: [6,9,1,2,1,1],
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
                    text: "elapsed time"
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
        
        let tableStructure = `
            <table id=mapred>
                <tr>
                    <th>Start Timestamp</th>
                    <th>End Timestamp</th>
                    <th>Elapsed Time</th>
                </tr>                        
                ${mapred.tableData.map(entry =>
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
    const ws = new WebSocket("ws://localhost:8000/ws")

    // websocket broadcast handler
    ws.onmessage = function(event) {
        const message = JSON.parse(event.data)
        if (message.type == "data"){
            appendRow("mapred", message.data)
            mapred.chartData.push(message.data[2])

            chartStatus = Chart.getChart("chart")
            if ( chartStatus != undefined){
                chartStatus.destroy()
                buildChart("chart")
            }
            
        }
        else {
            console.log(message.message)
        }
    }

    // retrieve csv file
    fetch("http://localhost:8000/static/mapred.csv",{method: "GET"})
    .then(
        data => data.text()
    )
    .then(
        dataText => {
            parseRawCSV(dataText)
            buildChart("chart")
            buildTable('mapred_table')
        }
    )
    .catch(
        error => console.log(error)
    )    
}