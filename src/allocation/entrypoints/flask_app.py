from datetime import datetime

from flask import Flask, jsonify, request

from allocation import views
from allocation.adapters import orm
from allocation.domain import commands
from allocation.service_layer import handlers, unit_of_work, messagebus

app = Flask(__name__)
orm.start_mappers()


@app.route("/add_batch", methods=['POST'])
def add_batch():
    eta = request.json['eta']
    if eta is not None:
        eta = datetime.fromisoformat(eta).date()
    cmd = commands.CreateBatch(request.json['ref'], request.json['sku'], request.json['qty'], eta)
    messagebus.handle(cmd, unit_of_work.SqlAlchemyUnitOfWork())
    return 'OK', 201


@app.route("/allocate", methods=['POST'])
def allocate_endpoint():
    try:
        cmd = commands.Allocate(
            request.json['orderid'], request.json['sku'], request.json['qty']
        )
        messagebus.handle(cmd, unit_of_work.SqlAlchemyUnitOfWork())
    except handlers.InvalidSku as e:
        return jsonify({'message': str(e)}), 400

    return 'OK', 202


@app.route("/allocations/<orderid>", methods=['GET'])
def allocations_view_endpoint(orderid):
    uow = unit_of_work.SqlAlchemyUnitOfWork()
    result = views.allocations(orderid, uow)
    if not result:
        return 'not found', 404
    return jsonify(result), 200
