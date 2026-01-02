import { useState, useEffect } from "react";

const API = "/api";

export default function App() {
  const [processId, setProcessId] = useState("6HTsgaZb77zwzQHm4");
  const [taskId, setTaskId] = useState(null);
  const [status, setStatus] = useState("idle");
  const [progress, setProgress] = useState({ percent: 0 });
  const [downloadUrl, setDownloadUrl] = useState(null);
  const [error, setError] = useState(null);

  async function trigger() {
    setError(null);
    setDownloadUrl(null);
    setStatus("triggering");

    const res = await fetch(`${API}/exports/trigger`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ processId }),
    });

    const data = await res.json();
    if (!res.ok) {
      setError(data.error || "Trigger failed");
      setStatus("idle");
      return;
    }

    setTaskId(data.taskId);
    setStatus("queued");
  }

  useEffect(() => {
    if (!taskId) return;

    const t = setInterval(async () => {
      const res = await fetch(`${API}/exports/status/${taskId}`);
      const data = await res.json();

      if (!res.ok) {
        setError(data.error || "Status error");
        return;
      }

      setStatus(data.status);
      setProgress(data.progress || { percent: 0 });
      setError(data.error || null);

      if (data.downloadUrl) {
        setDownloadUrl(data.downloadUrl);
        clearInterval(t);

        // auto-download
        window.location.href = data.downloadUrl;
      }
    }, 1000);

    return () => clearInterval(t);
  }, [taskId]);

  return (
    <div style={{ padding: 20, maxWidth: 700 }}>
      <h2>Async Export (Queue + Worker Streams JSON to S3)</h2>

      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
        <label>processId:</label>
        <input
          value={processId}
          onChange={(e) => setProcessId(e.target.value)}
          style={{ width: 320 }}
        />
        <button onClick={trigger}>Trigger Export</button>
      </div>

      <div style={{ marginTop: 20 }}>
        <div>
          <b>taskId:</b> {taskId || "-"}
        </div>
        <div>
          <b>status:</b> {status}
        </div>

        <div
          style={{
            marginTop: 10,
            height: 16,
            background: "#eee",
            borderRadius: 8,
            overflow: "hidden",
          }}
        >
          <div
            style={{
              width: `${progress?.percent || 0}%`,
              height: "100%",
              background: "#4caf50",
            }}
          />
        </div>
        <div>{progress?.percent || 0}%</div>

        {error && (
          <div style={{ color: "red", marginTop: 10 }}>Error: {error}</div>
        )}

        {downloadUrl && (
          <div style={{ marginTop: 10 }}>
            <a href={downloadUrl}>Download</a>
          </div>
        )}
      </div>
    </div>
  );
}
