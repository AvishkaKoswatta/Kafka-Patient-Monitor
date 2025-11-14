const eventSource = new EventSource("/stream");

eventSource.onmessage = function(event) {
    const data = JSON.parse(event.data);

    const table = document
        .getElementById("vitals-table")
        .getElementsByTagName("tbody")[0];

    const row = table.insertRow(0);

    const status =
        data.heart_rate > 100 || data.oxygen_saturation < 90
        ? "⚠️ ALERT"
        : "OK";

    row.innerHTML = `
        <td>${data.patient_id}</td>
        <td>${data.heart_rate}</td>
        <td>${data.blood_pressure_systolic}</td>
        <td>${data.blood_pressure_diastolic}</td>
        <td>${data.oxygen_saturation}</td>
        <td>${status}</td>
    `;
};
