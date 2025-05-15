from flask import Flask, request, jsonify
import subprocess
import tempfile
from pathlib import Path

app = Flask(__name__)

@app.route("/etl/run", methods=["POST"])
def run_etl():
    try:
        # Simulation : on utilise le fichier d’exemple intégré
        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = "tests/input1.xlsx"  # Doit exister dans l'image Docker
            output_path = Path(tmpdir) / "fusion.xlsx"

            result = subprocess.run([
                "python", "ETL_SIAMP.py",
                "--fichiers", input_file,
                "--chemin_sortie", str(output_path),
                "--taux_manuels", "USD=0.93",
                "--date", "2024-01-01",
                "--mois_selectionnes", "2025-01,2025-02"
            ], capture_output=True, text=True)

            if result.returncode != 0:
                return jsonify({"status": "error", "output": result.stderr}), 500

            return jsonify({"status": "success", "output": result.stdout})
    except Exception as e:
        return jsonify({"status": "exception", "message": str(e)}), 500

@app.route("/ping")
def ping():
    return jsonify({"message": "API up"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
