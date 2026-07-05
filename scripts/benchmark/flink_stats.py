import json
import urllib.request
import sys

def get_json(url):
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            return json.loads(r.read().decode('utf-8'))
    except Exception as e:
        print(f"Error calling {url}: {e}", file=sys.stderr)
        return None

def main():
    # Find running job
    jobs_info = get_json("http://localhost:8081/jobs/overview")
    if not jobs_info:
        print("No job info retrieved.")
        return 1
    
    running_jobs = [j for j in jobs_info.get("jobs", []) if j.get("state") == "RUNNING"]
    if not running_jobs:
        print("No running Flink jobs found.")
        return 1
    
    job = running_jobs[0]
    jid = job["jid"]
    print(f"Flink Job ID: {jid}")
    print(f"Job Name: {job['name']}")
    print(f"State: {job['state']}")
    print("-" * 50)
    
    # Get checkpoints stats
    chk = get_json(f"http://localhost:8081/jobs/{jid}/checkpoints")
    if chk:
        counts = chk.get("counts", {})
        print("Checkpoint Counts:")
        print(f"  Triggered: {counts.get('total')}")
        print(f"  Completed: {counts.get('completed')}")
        print(f"  Failed: {counts.get('failed')}")
        print(f"  In Progress: {counts.get('in_progress')}")
        
        summary = chk.get("summary", {})
        dur = summary.get("end_to_end_duration", {})
        print("\nCheckpoint Duration (End to End):")
        print(f"  Min: {dur.get('min')} ms")
        print(f"  Max: {dur.get('max')} ms")
        print(f"  Avg: {dur.get('avg')} ms")
        print(f"  p50: {dur.get('p50')} ms")
        print(f"  p95: {dur.get('p95')} ms")
        print(f"  p99: {dur.get('p99')} ms")
    
    # Get backpressure stats
    job_details = get_json(f"http://localhost:8081/jobs/{jid}")
    if job_details:
        vertices = job_details.get("vertices", [])
        print("\nBackpressure Status:")
        bp_levels = {}
        for v in vertices:
            vid = v["id"]
            name = v["name"].split(" -> ")[0] # simplify name
            bp = get_json(f"http://localhost:8081/jobs/{jid}/vertices/{vid}/backpressure")
            if bp:
                level = bp.get("backpressure-level", "unknown")
                bp_levels[name] = level
        
        # Print summary
        max_level = "ok"
        for name, level in bp_levels.items():
            print(f"  {name}: {level}")
            if level == "high":
                max_level = "high"
            elif level == "low" and max_level != "high":
                max_level = "low"
        
        print(f"\nMax Backpressure Level: {max_level.upper()}")

if __name__ == "__main__":
    sys.exit(main())
