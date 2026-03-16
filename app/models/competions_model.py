
from datetime import datetime
from app.extensions import db

class Country(db.Model):
    __tablename__ = 'countries'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)
    iso_code = db.Column(db.String(3), unique=True)

class Sport(db.Model):
    __tablename__ = 'sports'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), unique=True, nullable=False) 
    is_active = db.Column(db.Boolean, default=True)

class Competition(db.Model):
    __tablename__ = 'competitions'
    id = db.Column(db.Integer, primary_key=True)
    sport_id = db.Column(db.Integer, db.ForeignKey('sports.id'), nullable=False)
    country_id = db.Column(db.Integer, db.ForeignKey('countries.id'), nullable=True)
    name = db.Column(db.String(150), nullable=False) 
    gender = db.Column(db.String(10), default='M') # 'M', 'F', 'Mixed'
    
    sport = db.relationship('Sport', backref='competitions')
    country = db.relationship('Country', backref='competitions')

class Team(db.Model):
    __tablename__ = 'teams'
    id = db.Column(db.Integer, primary_key=True)
    sport_id = db.Column(db.Integer, db.ForeignKey('sports.id'), nullable=False)
    name = db.Column(db.String(150), nullable=False) 
    gender = db.Column(db.String(10), default='M') # 'M', 'F', 'Mixed'
    
    sport = db.relationship('Sport', backref='teams')