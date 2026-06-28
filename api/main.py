from flask import Flask, jsonify, request
from flask_cors import CORS
import os
import sys
import subprocess
from functools import wraps

# Ensuring the project root is in sys.path
project_root = os.getcwd()
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from dotenv import load_dotenv
load_dotenv(os.path.join(project_root, ".env"))

# --- Flask App Initialization ---
app = Flask(__name__)
CORS(app)

# --- Global Supabase Client ---
_supabase_client = None

def get_supabase():
    global _supabase_client
    if _supabase_client is None:
        # Import inside to avoid circular dependency if any
        from api.supabase_builder import get_supabase_client as init_client
        _supabase_client = init_client()
    return _supabase_client

# --- Dependency Injection Decorator ---
def with_supabase(f):
    """
    Decorator to inject a synchronous Supabase client into the route function.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        supabase = get_supabase()
        return f(supabase, *args, **kwargs)
    return decorated_function

def fetch_metrics(supabase, entity_name):
    """
    Helper to fetch metrics using an injected client.
    """
    try:
        response = supabase.table("metrics").select("*").eq("entity_name", entity_name).execute()
        metrics_dict = {}
        for row in response.data:
            metrics_dict[row['metric_type'].lower()] = row['data']
        return metrics_dict
    except Exception as e:
        print(f"Error fetching metrics for {entity_name}: {e}")
        return None

# --- ROUTES ---

@app.route('/api/', methods=['GET'])
def root():
    return jsonify({'message': 'Welcome to SarvSynth API', 'status': 'OK'}), 200

@app.route('/api/patients', methods=['GET'])
@with_supabase
def get_patients(supabase):
    """Return patients from Supabase"""
    try:
        limit = request.args.get('limit', default=100, type=int)
        response = supabase.table("patients").select("*").limit(limit).execute()
        return jsonify(response.data), 200
    except Exception as e:
        return jsonify({'error': f'Failed to retrieve patients: {str(e)}'}), 500

@app.route('/api/generate_data/', methods=['GET'])
def generate_data():
    """Trigger data generation script (either local or via GitHub Actions if configured)"""
    try:
        num_patients = request.args.get('num_patients', type=int)

        if num_patients is None or num_patients <= 0:
            raise ValueError("num_patients must be a positive integer")

        state = request.args.get('state', default=None, type=str)
        
        # Check if GitHub Action dispatch parameters are configured in the environment
        github_token = os.getenv("GITHUB_TOKEN")
        github_repo = os.getenv("GITHUB_REPOSITORY")
        github_ref = os.getenv("GITHUB_REF", "main")
        
        if github_token and github_repo:
            import urllib.request
            import json
            
            workflow_filename = "data_pipeline.yml"
            url = f"https://api.github.com/repos/{github_repo}/actions/workflows/{workflow_filename}/dispatches"
            
            # workflow dispatch inputs are always string mappings
            payload = {
                "ref": github_ref,
                "inputs": {
                    "num_patients": str(num_patients),
                    "region": state if state else "Massachusetts"
                }
            }
            
            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(
                url,
                data=data,
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {github_token}",
                    "X-GitHub-Api-Version": "2022-11-28",
                    "User-Agent": "SarvSynth-App",
                    "Content-Type": "application/json"
                },
                method="POST"
            )
            
            try:
                with urllib.request.urlopen(req) as response:
                    if response.status == 204:
                        return jsonify({
                            "status": "success",
                            "message": f"GitHub Action successfully triggered: Generating {num_patients} patients in {state if state else 'Massachusetts'} region asynchronously on GitHub runners."
                        }), 200
                    else:
                        return jsonify({
                            "status": "error",
                            "message": f"Failed to trigger GitHub Action: {response.status} {response.reason}"
                        }), response.status
            except Exception as github_err:
                return jsonify({
                    "status": "error",
                    "message": f"GitHub Actions Dispatch failed: {str(github_err)}"
                }), 500
                
        # Otherwise fallback to local subprocess generation
        script_path = os.path.join(project_root, "workflows", "scripts", "synthea-init.sh")

        if not os.path.exists(script_path):
            return jsonify({"status": "error", "message": "Synthea script not found"}), 404

        # Pass state as the second argument if provided
        cmd = [script_path, str(num_patients)]
        if state:
            cmd.append(state)
            
        subprocess.run(cmd, check=True)
        
        return jsonify({
            "status": "success",
            "message": f"Local run triggered: {num_patients} patient records generated locally"
        }), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/patient_dashboard', methods=['GET'])
@with_supabase
def patient_dashboard(supabase):
    """Fetch patient metrics from Supabase"""
    metrics = fetch_metrics(supabase, "patients")
    if not metrics:
        return jsonify({'message': 'Data not found. Run analytics pipeline first.'}), 404
    
    adv_metrics = metrics.get('advanced_metrics', {})
    
    return jsonify({
        'message': 'Data Loaded successfully',
        'kpis': metrics.get('kpis', {}),
        'metrics': metrics.get('metrics', {}),
        'metric_trends': {
            'economic_dependence': adv_metrics.get('economic_dependence_trend', []),
            'cultural_diversity': adv_metrics.get('cultural_diversity_trend', []),
            'mortality_rate': adv_metrics.get('mortality_rate_trend', [])
        },
        'advanced_metrics': adv_metrics
    })

@app.route('/api/conditions_dashboard', methods=['GET'])
@with_supabase
def conditions_dashboard(supabase):
    """Fetch condition metrics from Supabase"""
    metrics = fetch_metrics(supabase, "conditions")
    if not metrics:
        return jsonify({'message': 'Data not found. Run analytics pipeline first.'}), 404
    
    return jsonify({
        'message': 'Data Loaded successfully',
        'kpis': metrics.get('kpis', {}),
        'metrics': metrics.get('metrics', {}),
        'advanced_metrics': metrics.get('advanced_metrics', {})
    })

@app.route('/api/encounters_dashboard', methods=['GET'])
@with_supabase
def encounters_dashboard(supabase):
    """Fetch encounter metrics from Supabase"""
    metrics = fetch_metrics(supabase, "encounters")
    if not metrics:
        return jsonify({'message': 'Data not found. Run analytics pipeline first.'}), 404
    
    return jsonify({
        'message': 'Data Loaded successfully',
        'kpis': metrics.get('kpis', {}),
        'metrics': metrics.get('metrics', {}),
        'advanced_metrics': metrics.get('advanced_metrics', {})
    })

@app.route('/api/allergy_dashboard', methods=['GET'])
@with_supabase
def allergy_dashboard(supabase):
    """Fetch encounter metrics from Supabase"""
    metrics = fetch_metrics(supabase, "allergies")
    if not metrics:
        return jsonify({'message': 'Data not found. Run analytics pipeline first.'}), 404
    
    return jsonify({
        'message': 'Data Loaded successfully',
        'kpis': metrics.get('kpis', {}),
        'metrics': metrics.get('metrics', {}),
        'advanced_metrics': metrics.get('advanced_metrics', {})
    })
    
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=3001, debug=True)
