import sys
import logging
from flask import Flask, request, jsonify
from node import Node, PRIMARY, REPLICA
import logger
from prometheus_client import make_wsgi_app, REGISTRY
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.serving import run_simple

app = Flask(__name__)

# Suppress Flask's default access log noise
log = logging.getLogger('werkzeug')
log.disabled = True

# -------------------------------------------------------------------
# CLIENT-FACING ROUTES
# -------------------------------------------------------------------

@app.route("/request", methods=["GET"])
def value_get():
    payload = request.json.get("payload", {})
    result  = n.handle_get(payload)
    if result:
        return jsonify({"code": "success", "payload": result})
    return jsonify({"code": "fail", "payload": payload})


@app.route("/request", methods=["PUT"])
def value_put():
    payload = request.json.get("payload", {})

    # Replicas do not accept writes directly — redirect to primary
    if n.role == REPLICA:
        return jsonify({"code": "redirect", "primary": primary_addr})

    result = n.handle_put(payload)
    if result:
        return jsonify({"code": "success"})
    return jsonify({"code": "fail"})


# -------------------------------------------------------------------
# INTER-NODE ROUTES (primary <-> replica)
# -------------------------------------------------------------------

@app.route("/replicate", methods=["POST"])
def replicate():
    """Primary sends log/commit messages here."""
    msg      = request.json
    commitIdx = n.receive_replicate(msg)
    return jsonify({"commitIdx": commitIdx})


@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    """Primary pings replicas here to check liveness."""
    msg      = request.json
    commitIdx = n.receive_heartbeat(msg)
    return jsonify({"commitIdx": commitIdx})


# -------------------------------------------------------------------
# OBSERVABILITY
# -------------------------------------------------------------------

@app.route("/metrics", methods=["GET"])
def metrics():
    """Returns node health, op counts, replica status, and commit index."""
    return jsonify(n.metrics())


# -------------------------------------------------------------------
# ENTRY POINT
# -------------------------------------------------------------------

if __name__ == "__main__":
    # Usage:
    #   Primary : python server.py primary http://127.0.0.1:5000 http://127.0.0.1:5001 http://127.0.0.1:5002
    #   Replica : python server.py replica http://127.0.0.1:5001 http://127.0.0.1:5000

    if len(sys.argv) < 3:
        print("Usage:")
        print("  Primary : python server.py primary <my_addr> <replica1> [<replica2> ...]")
        print("  Replica : python server.py replica <my_addr> <primary_addr>")
        sys.exit(1)

    role    = sys.argv[1]          # "primary" or "replica"
    my_ip   = sys.argv[2]          # this node's own address

    if role == PRIMARY:
        replicas     = sys.argv[3:]    # all replica addresses
        primary_addr = my_ip
        wal_path     = f"wal_primary.log"
    else:
        replicas     = []
        primary_addr = sys.argv[3] if len(sys.argv) > 3 else ""
        port         = my_ip.split(":")[-1]
        wal_path     = f"wal_replica_{port}.log"

    # Init structured logger before anything else
    logger.init(my_ip)

    n = Node(
        role=role,
        my_ip=my_ip,
        replicas=replicas,
        wal_filepath=wal_path,
    )

    _, host, port = my_ip.split(":")
    # Mount Prometheus metrics at /metrics-prom (keep your JSON /metrics separate)
    app_dispatch = DispatcherMiddleware(app, {
        '/metrics-prom': make_wsgi_app()
    })
    run_simple("0.0.0.0", int(port), app_dispatch, use_reloader=False)