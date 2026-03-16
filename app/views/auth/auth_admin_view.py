from flask import Blueprint, request, jsonify
from flask_jwt_extended import create_access_token
from werkzeug.security import check_password_hash
from app.models.user_admin import User
from . import authorization


@authorization.route('/login', methods=['POST'])
def login():
    data = request.json
    email = data.get('email')
    password = data.get('password')

    user = User.query.filter_by(email=email).first()
    if not user or not check_password_hash(user.password_hash, password):
        return jsonify({"msg": "Bad email or password"}), 401

    # Add role to JWT claims for RBAC (Role-Based Access Control)
    access_token = create_access_token(identity=str(user.id), additional_claims={"role": "admin"})
    return jsonify(access_token=access_token, role="admin"), 200